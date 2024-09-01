#!/bin/bash

# Create necessary directories in HDFS
hadoop fs -mkdir -p /tmp
hadoop fs -mkdir -p /user/hive/warehouse

# Set permissions
hadoop fs -chmod g+w /tmp
hadoop fs -chmod g+w /user/hive/warehouse

# Start HiveServer2
cd $HIVE_HOME/bin
./hiveserver2 --hiveconf hive.server2.enable.doAs=false
