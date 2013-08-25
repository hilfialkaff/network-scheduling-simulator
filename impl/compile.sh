#!/bin/bash

cd ./tests

function terasort {
    cd ./terasort/
    javac -d terasort_classes *.java
    jar -cvf ./terasort.jar -C terasort_classes/ .
    cd ..
}

function mrbench {
    cd mapred
    javac -d mapred_classes MRBench.java
    jar -cvf ./mrbench.jar -C mapred_classes/ .
    cd ..
}

function wordcount {
    cd wordcount
    javac -d wordcount_classes WordCount.java
    jar -cvf ./wordcount.jar -C wordcount_classes/ .
    cd ..
}

# terasort
mrbench
# wordcount
