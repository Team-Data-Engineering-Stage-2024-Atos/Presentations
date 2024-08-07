#!/bin/bash

# Compile the Java code
javac -classpath $(hadoop classpath) -d . SplitUSDataMapper.java SplitUSDataReducer.java SplitUSDataDriver.java
jar cf splitusdata.jar SplitUSData*.class

# Define input and output paths in HDFS
INPUT_PATH="/user/hadoop/datasets/us_raw"
OUTPUT_PATH="/user/hadoop/datasets/us_cleaned"

# Run the Hadoop job
hadoop jar splitusdata.jar SplitUSDataDriver $INPUT_PATH $OUTPUT_PATH
