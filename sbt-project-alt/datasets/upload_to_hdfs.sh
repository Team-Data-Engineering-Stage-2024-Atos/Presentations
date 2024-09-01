#!/bin/bash

hdfs dfs -mkdir -p /data/raw/
hdfs dfs -put customers.csv /data/raw/
hdfs dfs -put products.csv /data/raw/
hdfs dfs -put sales.csv /data/raw/
hdfs dfs -put dates.csv /data/raw/