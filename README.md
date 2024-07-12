# datax_autorun

# 1. 概览
    用户mysql到mysql数据库datax同步.从数据库中读取库和表信息,自动生成datax配置文件,然后并发执行datax.

3. 优点
    * 只需要输入少部分信息,自动运行datax迁移数据.
    * 读取源库元数据生成datax配置文件,大量数据文件在很短时间内生成完成,避免手动添加出错.
    * 可以添加不迁移的库和不迁移的表.
    * 自由设置并发量,更快的完成迁移工作
    * 各个操作步骤均保存日志,可快速定位问题

4. 使用方法:
    * 修改配置文件auto_run.conf.
    * 执行python3 main.py --conf=/home/mysql/ityunqianyi/218_155_3306/datax_auto/auto_run.conf

5. 日志系统:
    运行时,在程序目录下自动生成日志文件autorun.log,保存每个步骤日志.
    1. 生成datax配置文件,write datax config ___.json finished
    2. 开始执行datax,INFO - begin exceute ___.json
    3. 正常完成,INFO - ___.json execute finished.log file:___.log
    4. 报错退出,ERROR - ___.json exectue failed,see log: ___.log
    5. datax执行文件日志记录在配置文件的log_dir中
