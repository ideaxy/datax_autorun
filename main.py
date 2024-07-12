#!/usr/bin/env python
# -*- coding:utf-8 -*-

import getopt
import sys
import configparser
import pymysql
from datax_t import put_config
import os
import subprocess
from multiprocessing import Pool
from log import logger



def execute_command(command):
    log_file_convert = str(command).replace("json","log")
    log_file = os.path.join(config["log_dir"],log_file_convert)
    with open(log_file,mode="a+") as log:
        comm_list = [config["datax_dir"]]
        command = os.path.join(config["conf_dir"],command)
        comm_list.append(command)
        logger.info("begin exceute %s",command)
        process = subprocess.Popen(comm_list,text=True,stdout=log,stderr=log)
        process.communicate()
        returncode = process.returncode
        if returncode == 0:
            logger.info("%s execute finished.log file:%s" %(command,log_file))
        else:
            logger.error("%s exectue failed,see log: %s" % (command,log_file))

def run_commands(cmd_lst, max_concurrency):
    pool = Pool(processes=max_concurrency)
    pool.map(execute_command, cmd_lst)
    pool.close()
    pool.join()


class put_data(object):
    def __init__(self,host,port,user,passwd):
        try:
            self.conn = pymysql.connect(host=host,port=port,user=user,passwd=passwd)
        except Exception as e:
            logger.error(e)
            print(e)
            return False
    def sql_exec(self,sql):
        self.cursor = self.conn.cursor()
        self.cursor.execute(sql)
        self.data = self.cursor.fetchall()
        if(len(self.data) == 0):
            logger.info("not found data in source database.")
            print("not found data in source database.")
        
        return self.data
    def put_datas(self):
        return self.data
        
    def close_conn(self):
        self.cursor.close()
        self.conn.close()



def par_config(config_path):
    conf = configparser.ConfigParser()
    try:
        conf.read(config_path)
    except Exception as e:
        print("open config file failed.")
        exit(-1)
    run_conf_dict = dict(conf.items("auto_run"))
    return(run_conf_dict)
    

# 使用方法
def usage():
    print("Usage./%s --config=config_file.conf" %sys.argv[0])


if __name__ == "__main__":
    config = {}
    datax_conf_list = []

    opts,args = getopt.getopt(sys.argv[1:],"hs:c:",["help","config="])
    if(len(opts)) == 0:
        usage()
        exit(1)
    for key,value in opts:
        if key in ("-h","--help"):
            usage()
            exit(1)
        if key in ("-f","--config"):
            config =  par_config(value)

    try:
        db_file = open(config["get_db_dir"],mode="a+")
    except Exception as e:
        logger.error("%s,check <get_db_dir> in config file." %e)
    else:
        max_concurrency = int(config["thread"])
        ex_db = "'sys','mysql','performance_schema','information_schema'"
        if len(config["ex_db"]) != 0:
            ex_db = ex_db + ',' + config["ex_db"]
        
        if len(config["ex_table"]) == 0:
            sql = f"select TABLE_SCHEMA,TABLE_NAME from information_schema.TABLES where TABLE_SCHEMA not in ({ex_db})"
        elif len(config["ex_table"]) != 0:
            ex_table = config["ex_table"]
            sql = f"select TABLE_SCHEMA,TABLE_NAME from information_schema.TABLES where TABLE_SCHEMA not in ({ex_db}) and TABLE_NAME not in ({ex_table})"


        get_table = put_data(config["sour_ip"],int(config["sour_port"]),config["sour_user"],config["sour_passwd"])
        get_table.sql_exec(sql)
        data = get_table.put_datas()
        get_table.close_conn()
        for i in data:
            # 读取处理的库和表名
            db_name = i[0]
            table_name = i[1]

            db_file.write(db_name + "." + table_name + "\n")
            
            datax_json = db_name + "." + table_name + ".json"
            datax_full_json = os.path.join(config["conf_dir"],datax_json)
            

            # 生成datax配置文件
            datax_conf = put_config(config["sour_ip"],config["sour_port"],config["sour_user"],config["sour_passwd"],config["tar_ip"],config["tar_port"],config["tar_user"],config["tar_passwd"],config["presql"],db_name,table_name)

            try:
                with open(datax_full_json,mode='w+',encoding='UTF-8') as datax_file:
                    datax_conf_list.append(datax_json)
                    datax_file.write(datax_conf)
                    logger.info("write datax config %s finished" %(datax_full_json))
            except Exception as e:
                logger.error("%s,check <conf_dir> in config file." %e)
        db_file.close()


        # 并行执行datax
        run_commands(datax_conf_list,max_concurrency)
