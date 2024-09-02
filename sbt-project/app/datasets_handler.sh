#!/bin/bash

hdfs dfs -mkdir -p /user/hadoopuser/datasets
hdfs dfs -put ./datasets/* /user/hadoopuser/datasets
