#!/bin/bash

# Compile the Java code
javac -classpath $(hadoop classpath) -d . CleanAndProcessDataMapper.java CleanAndProcessDataReducer.java CleanAndProcessDataDriver.java
jar cf cleanandprocessdata.jar CleanAndProcessData*.class

# Define input and output paths in HDFS
INPUT_PATH="/user/hadoop/datasets/raw"
OUTPUT_PATH="/user/hadoop/datasets/cleaned"

# Run the Hadoop job
hadoop jar cleanandprocessdata.jar CleanAndProcessDataDriver $INPUT_PATH $OUTPUT_PATH
