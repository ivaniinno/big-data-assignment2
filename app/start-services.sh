#!/bin/bash
# This will run only by the master node

# starting HDFS daemons
$HADOOP_HOME/sbin/start-dfs.sh

# starting Yarn daemons
$HADOOP_HOME/sbin/start-yarn.sh
# yarn --daemon start resourcemanager

# Start mapreduce history server
mapred --daemon start historyserver


# track process IDs of services
jps -lm

# subtool to perform administrator functions on HDFS
# outputs a brief report on the overall HDFS filesystem
hdfs dfsadmin -report

# If namenode in safemode then leave it
hdfs dfsadmin -safemode leave

# create a directory for spark apps in HDFS
hdfs dfs -mkdir -p /apps/spark
hdfs dfs -chmod 744 /apps/spark

# Package Spark jars as a single archive for faster YARN localization
rm -f /tmp/spark-jars.jar
jar cf /tmp/spark-jars.jar -C /usr/local/spark/jars .
hdfs dfs -put -f /tmp/spark-jars.jar /apps/spark/spark-jars.jar


# print version of Scala of Spark
scala -version

# track process IDs of services
jps -lm

# Create a directory for root user on HDFS
hdfs dfs -mkdir -p /user/root
