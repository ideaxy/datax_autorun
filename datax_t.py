
# datax template

def put_config(sour_ip,sour_port,sour_user,sour_passwd,tar_ip,tar_port,tar_user,tar_passwd,preSql,db_name,table_name):
  
  sour_db_connect = sour_ip + ':' + sour_port
  tar_db_connect = tar_ip + ':' + tar_port

  config_text = f"""
{{"job": {{
  "setting": {{
    "speed": {{
          "channel": 10,
          "byte": -1,
          "record": -1,
    }}
  }},
  "content": [
    {{
          "reader": {{
            "name": "mysqlreader",
            "parameter": {{
                  "username": "{sour_user}",
                  "password": "{sour_passwd}",
      "column":["*"],
      "splitPk": "",
                  "connection": [
                    {{
                          "table": [
                            "{table_name}"
                          ],
                          "jdbcUrl": [
                            "jdbc:mysql://{sour_db_connect}/{db_name}"
                          ]
                    }}
                  ]
            }}
          }},
          "writer": {{
            "name": "mysqlwriter",
            "parameter": {{
                  "writeMode": "insert",
                  "username": "{tar_user}",
                  "password": "{tar_passwd}",
                  "column": [
                    "*"
                  ],
                  "session": [
                    "set session sql_mode='ANSI'"
                  ],
                  "preSql": [
                    "{preSql}"
                  ],
                  "connection": [
                    {{
                          "jdbcUrl": "jdbc:mysql://{tar_db_connect}/{db_name}",
                          "table": [
                            "{table_name}"
                          ]
                    }}
                  ]
            }}
          }}
    }}
  ]
}}
}}
"""

  return config_text
