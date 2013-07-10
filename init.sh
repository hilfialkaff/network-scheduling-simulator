#!/bin/bash

source env.sh

echo "master" > conf/slaves
cat /etc/hosts | awk '{print $4}' | grep client | grep -v client-1 >> conf/slaves
cp conf $HADOOP_HOME -r

# rm -rf $HDFS_PATH
hadoop namenode -format

start-dfs.sh
sleep 5
start-mapred.sh
