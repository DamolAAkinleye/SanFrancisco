
#Version: 1.0
FROM ubuntu:16.04

MAINTAINER Alejandro Reina Reina "alex.konu@gmail.com"

#Actualizacion de la lista de paquetes y "actualizaciones" inicial
RUN apt-get update

###############################JAVA##############################
#Version de Java: 8
#Instalamos los paquetes necesarios para ejecutar el comando apt-add-repository y resincronizamos los paquetes
RUN apt-get install -y software-properties-common && \apt-get update
#Añadimos el repositorio de Java y resincronizamos paquetes
RUN apt-add-repository -y ppa:webupd8team/java && \apt-get update
#Establecemos opciones de configuracion para aceptar la licencia de java automaticamente
RUN echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections 
#Instalar Java
RUN apt-get install -y oracle-java8-installer
#Establecer PATH JAVA
ENV export JAVA_HOME = /usr/lib/jvm/java-8-oracle
#Prueba de vida
RUN java -version

#############################SCALA###################################
#Version de scala: 2.11.6

#Descarga de Scala
RUN wget http://www.scala-lang.org/files/archive/scala-2.11.6.tgz

#Descomprimimos, movemos al directorio /usr/local y liberamos espacio del tgz
RUN tar xvf scala-2.11.6.tgz && \
	mv scala-2.11.6 /usr/local/scala && \
	rm -R scala-2.11.6.tgz
#Establecemos PATH de Scala
ENV SCALA_HOME=/usr/local/scala
ENV PATH=$SCALA_HOME/bin:$PATH

#Prueba de vida
RUN scala -version

############################APACHE SPARK ################
#Version de Spark: 2.2.0(Jul 11 2017) con Pre-built para Hadoop 2.7

#Descarga de Spark
RUN wget https://d3kbcqa49mib13.cloudfront.net/spark-2.2.0-bin-hadoop2.7.tgz

#Descomprimir, mover al directorio /usr/local/spark y liberar espacio
RUN tar xvf spark-2.2.0-bin-hadoop2.7.tgz && \
	mv spark-2.2.0-bin-hadoop2.7 /usr/local/spark && \
	rm -R spark-2.2.0-bin-hadoop2.7.tgz
#Establecer PATH de Spark
ENV SPARK_HOME=/usr/local/spark
ENV PATH=$SPARK_HOME/bin:$PATH

COPY spark-env.sh /usr/local/spark/conf

#Prueba de vida
RUN spark-shell

######################### HBASE #############
#version de hbase 1.2.6

#Descarga de Hbase
RUN wget http://apache.rediris.es/hbase/1.2.6/hbase-1.2.6-bin.tar.gz

#Descomprimir y mover al directorio /usr/local/hbase
RUN tar xvf hbase-1.2.6-bin.tar.gz && \
	mv hbase-1.2.6 /usr/local/hbase && \
	rm -R hbase-1.2.6-bin.tar.gz

#Copiamos ficheros de configuracion, modo STANDALONE
#Este fichero se ha modificado para añadir el PATH de JAVA
COPY hbase-env.sh /usr/local/hbase/conf
#Este fichero establece donde hbase guardara los archivos
# y los archivos de zookeeper
COPY hbase-site.xml /usr/local/hbase/conf

#Establecemos PATH de HBase
ENV HBASE_HOME=/usr/local/hbase
ENV PATH=$HBASE_HOME/bin:$PATH

#Arrancamos el servicio de HBase
RUN /usr/local/hbase/bin/start-hbase.sh

#####################################HIVE#########################
#version de hive:2.1.1

#DEPENDENCIAS
#Notas de la instalacion: Hive requiere librerias de hadoop para su funcionamiento
# ademas sustituiremos el motor derby de Hive por MySQL
#############HADOOP################
#version de hadoop: 2.7.4
#Descarga de Hadoop
RUN wget https://archive.apache.org/dist/hadoop/core/hadoop-2.7.4/hadoop-2.7.4.tar.gz
#Descomprimir y mover al directorio /usr/local/hadoop
RUN tar xvf hadoop-2.7.4.tar.gz && \
	rm -R hadoop-2.7.4.tar.gz && \
	mv hadoop-2.7.4 /usr/local/hadoop
#Fichero modificado para añadir el PATH de java en Hadoop
COPY hadoop-env.sh /usr/local/hadoop/etc/hadoop
#Establecer PATH de hadoop
ENV HADOOP_HOME=/usr/local/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin
ENV PATH=$PATH:$HADOOP_HOME/sbin
ENV HADOOP_MAPRED_HOME=$HADOOP_HOME
ENV HADOOP_COMMON_HOME=$HADOOP_HOME
ENV HADOOP_HDFS_HOME=$HADOOP_HOME
ENV YARN_HOME=$HADOOP_HOME
ENV HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native

#####MYSQL######
#version de mysql:5.7
#Establecer debconf para mysql
RUN echo "mysql-server-5.7 mysql-server/root_password password bigdata" | debconf-set-selections
RUN echo "mysql-server-5.7 mysql-server/root_password_again password bigdata" | debconf-set-selections
#Instalacion de mysql
RUN apt-get update &&  apt-get install -y mysql-server
#creamos usuario para Hive en MySQL
ADD mysql-hive.sql /usr/local/
RUN service mysql start && \
    mysql -u root -pbigdata < /usr/local/mysql-hive.sql
#####################HIVE##########
#Descarga de Hive
RUN wget https://apache.rediris.es/hive/hive-2.1.1/apache-hive-2.1.1-bin.tar.gz
#Descomprimir y mover al directorio /usr/local/hive
RUN tar xvf apache-hive-2.1.1-bin.tar.gz && \
	rm -R apache-hive-2.1.1-bin.tar.gz && \
	mv apache-hive-2.1.1-bin /usr/local/hive
#Establecer el PATH de Hive
ENV HIVE_HOME=/usr/local/hive
ENV PATH=$HIVE_HOME/bin:$PATH
#Establecemos path adicionales de Hive para Kylin
ENV HIVE_CONF=/usr/local/hive/conf
ENV HCAT_HOME=/usr/local/hive/hcatalog
ENV HIVE_LIB=/usr/local/hive/lib

#Fichero de configuracion de hive con el PATH DE hadoop
COPY hive-config.sh /usr/local/hive/bin
#Fichero de configuracion hive site
COPY hive-site.xml /usr/local/hive/conf
#Integracion de Hive con MySQL y Spark
#Librerias Java para MySQL
RUN apt-get install libmysql-java && \
	ln -s /usr/share/java/mysql-connector-java.jar /usr/local/hive/lib/
#Librerias Hive para Spark
RUN cp /usr/local/hive/lib/mysql-connector-java.jar /usr/local/spark/jars/ && \
	cp /usr/local/hive/lib/hive-jdbc-2.1.1.jar /usr/local/spark/jars/

#Borramos la bd por defecto, ya que da problemas
RUN rm -R metastore_db
#Inicializamos la bd por defecto a MySQL
RUN service mysql start && \
	/usr/local/hive/bin/schematool -initSchema -dbType mysql
#Prueba de vida
RUN service mysql start && \
	/usr/local/hive/bin/hive

###########################ZEPPELIN###########################
#Version: 7.3.0

ADD zeppelin /usr/local/zeppelin
#Ficheros modificados:
#zeppelin-env.sh establecemos PATH de Java
#zeppelin-site.xml desactivamos el acceso anonimo a la aplicacion
#shiro.ini informacion de usuarios por defecto admin/password1


########################KYLIN###########################
#version 2.2.0
#Descargamos Kylin, descomprimimos y movemos al directorio /usr/local/
RUN wget http://ftp.cixug.es/apache/kylin/apache-kylin-2.2.0/apache-kylin-2.2.0-bin-hbase1x.tar.gz && \
	tar -zxvf apache-kylin-2.2.0-bin-hbase1x.tar.gz && rm -R apache-kylin-2.2.0-bin-hbase1x.tar.gz && \
	mv apache-kylin-2.2.0-bin /usr/local/kylin/
#Establecemos variables de entorno
ENV KYLIN_HOME=/usr/local/kylin
ENV PATH=$KYLIN_HOME/bin:$PATH
#Añadimos script de inicio del contenedor
ADD init.sh /usr/local/
#Otorgamos permisos al fichero
RUN chown root:root /usr/local/init.sh
RUN chmod 700 /usr/local/init.sh
#Abrimos puertos de los servicios
#Spark 4040
#HBase 16010
#Hive 10002
#Zeppelin 8080
#Kylin 7070
EXPOSE 4040 7070 8080 10002 16010
#Automatizamos el arranque de los servicios cuando iniciemos el contenedor
CMD /usr/local/init.sh && /bin/bash
