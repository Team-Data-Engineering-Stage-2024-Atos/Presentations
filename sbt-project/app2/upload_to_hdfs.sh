#!/bin/bash

hdfs dfs -mkdir -p /data/raw/
hdfs dfs -put datasets/customers.csv /data/raw/
hdfs dfs -put datasets/products.csv /data/raw/
hdfs dfs -put datasets/sales.csv /data/raw/
hdfs dfs -put datasets/dates.csv /data/raw/