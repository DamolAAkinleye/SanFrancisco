service mysql start
/usr/local/hbase/bin/start-hbase.sh
hive --service hiveserver2 &
sleep 20
/usr/local/zeppelin/bin/zeppelin-daemon.sh start
/usr/local/kylin/bin/kylin.sh start


