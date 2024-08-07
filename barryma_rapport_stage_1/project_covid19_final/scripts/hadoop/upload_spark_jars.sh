#!/bin/bash

hdfs dfs -mkdir -p /user/spark/jars
hdfs dfs -put /opt/spark/jars/*.jar /user/spark/jars/
