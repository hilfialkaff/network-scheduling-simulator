#!/bin/bash

source env.sh

echo -e "10.1.1.2\tmaster" | sudo tee -a /etc/hosts
echo "master" > conf/slaves
cat /etc/hosts | awk '{print $4}' | grep client >> conf/slaves
cp conf $HADOOP_HOME -r

hadoop namenode -format
start-dfs.sh
start-mapred.sh
