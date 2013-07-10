#!/bin/bash

source env.sh

SRC_DIR=`pwd`
TEST_DIR=$SRC_DIR/tests
LOG_DIR=$SRC_DIR/logs

# DFS IO benchmark
function run_dfsio_bm {
    echo "Running dfsio benchmark..."
    for i in {20..100..20}
    do
        for j in {1..5..1}
        do
            echo -e "\n"$i $j >> $LOG_DIR/dfs_io.log
            (time hadoop jar hadoop-*test*.jar TestDFSIO -write -nrFiles $i -fileSize 100 &> $LOG_DIR/hadoop.log) >> $LOG_DIR/dfs_io.log 2>&1
            (time hadoop jar hadoop-*test*.jar TestDFSIO -read -nrFiles $i -fileSize 100 &> $LOG_DIR/hadoop.log) >> $LOG_DIR/dfs_io.log 2>&1
            hadoop jar hadoop-*test*.jar TestDFSIO -clean &> /dev/null
        done
    done
}


# Terasort benchmark
function run_terasort_bm {
    echo "Running terasort benchmark..."
    cd ./terasort/terasort_classes/
    for i in {100000000..100000000..10000000}
    do
        hadoop jar ../terasort.jar org.apache.hadoop.examples.terasort.TeraGen $i /data/input
        # hadoop jar hadoop-*examples*.jar teragen $i /data/input &> $LOG_DIR/hadoop.log
        for j in {1..1..1}
        do
            echo -e "\n"$i $j >> $LOG_DIR/terasort.log
            # (time hadoop jar ../terasort.jar org.apache.hadoop.examples.terasort.TeraSort /data/input /data/output &> $LOG_DIR/hadoop.log) >> $LOG_DIR/terasort.log 2>&1
            hadoop jar ../terasort.jar org.apache.hadoop.examples.terasort.TeraSort /data/input /data/output
            # (time hadoop jar hadoop-*examples*.jar terasort /data/input /data/output &> $LOG_DIR/hadoop.log) >> $LOG_DIR/terasort.log 2>&1
            hadoop dfs -rmr /data/output
        done
        hadoop dfs -rmr /data/input
    done
    
    cd $TEST_DIR
}

function run_mrbench_bm {
    echo "Running mrbench benchmark..."
    cd ./mapred/mapred_classes
    # (time hadoop jar hadoop-*test*.jar mrbench -numRuns 50) &>> $LOG_DIR/mrbench.log
    hadoop jar ../mrbench.jar org.apache.hadoop.mapred.MRBench -numRuns 50 -maps 5 -reduces 5 -inputType "random" -inputLines 1000000 &> $LOG_DIR/mrbench.log

    cd $TEST_DIR
}

function run_wordcount_bm {
    echo "Running wordcount benchmark..."
    cd ./wordcount/wordcount_classes/
    hadoop dfs -copyFromLocal ../input /data/wc_input
    hadoop jar ../wordcount.jar org.apache.hadoop.examples.WordCount -numRun 20 -in /data/wc_input -out /data/wc_output -D mapred.map.tasks = 20 -D mapred.reduce.tasks = 20 # 2> $LOG_DIR/wordcount.log

    cd $TEST_DIR
}

# Clean up
rm $SRC_DIR/logs/* -f
rm $HADOOP_HOME/logs/* -rf
hadoop dfs -rmr /data
cd tests

# run_dfsio_bm
# run_terasort_bm
run_mrbench_bm
# run_wordcount_bm
