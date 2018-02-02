create database metastore;
use metastore;
CREATE USER 'hive'@'%' IDENTIFIED BY 'bigdata';
GRANT all on *.* to 'hive'@localhost identified by 'bigdata';
flush privileges;
