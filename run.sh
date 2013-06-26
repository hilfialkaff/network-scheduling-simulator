#!/bin/bash

source env.sh

SRC_DIR=`pwd`
LOG_DIR=$SRC_DIR/logs

# DFS IO benchmark
function run_dfsio_bm {
    echo "Running dfsio benchmark..."
    for i in {1..2..1}
    do
        for j in {10..20..10}
        do
            echo -e "\n"$i $j >> $LOG_DIR/dfs_io.log
            (time hadoop jar hadoop-*test*.jar TestDFSIO -write -nrFiles $i -fileSize $j &> $LOG_DIR/hadoop.log) >> $LOG_DIR/dfs_io.log 2>&1
            (time hadoop jar hadoop-*test*.jar TestDFSIO -read -nrFiles $i -fileSize $j &> $LOG_DIR/hadoop.log) >> $LOG_DIR/dfs_io.log 2>&1
            hadoop jar hadoop-*test*.jar TestDFSIO -clean &> /dev/null
        done
    done
}


# Terasort benchmark
function run_terasort_bm {
    echo "Running terasort benchmark..."
    for i in {100000..200000..100000}
    do
        hadoop jar hadoop-*examples*.jar teragen $i /data/input &> /dev/null
        for j in {1..2..1}
        do
            echo -e "\n"$i $j >> $LOG_DIR/terasort.log
            (time hadoop jar hadoop-*examples*.jar terasort /data/input /data/output &> $LOG_DIR/hadoop.log) >> $LOG_DIR/terasort.log 2>&1
            hadoop dfs -rmr /data/output &> /dev/null
        done
        hadoop dfs -rmr /data/input &> /dev/null
    done
}

function run_mrbench_bm {
    echo "Running mrbench benchmark..."
    (time hadoop jar hadoop-*test*.jar mrbench -numRuns 3) &>> $LOG_DIR/mrbench.log
}

# Clean up
rm $SRC_DIR/logs/* -f
cd $HADOOP_HOME
hadoop dfs -rmr /data/

run_dfsio_bm
run_terasort_bm
# run_mrbench_bm
