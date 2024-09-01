#!/bin/bash

docker build -t barryma22/hadoop-base:redhat_ubi8 docker/hadoop/base
docker build -t barryma22/hadoop-namenode:redhat_ubi8-hadoop3.2.1-java8 docker/hadoop/namenode
docker build -t barryma22/hadoop-datanode:redhat_ubi8-hadoop3.2.1-java8 docker/hadoop/datanode
docker build -t barryma22/hadoop-resourcemanager:redhat_ubi8-hadoop3.2.1-java8 docker/hadoop/resourcemanager
docker build -t barryma22/hadoop-nodemanager:redhat_ubi8-hadoop3.2.1-java8 docker/hadoop/nodemanager
docker build -t barryma22/hadoop-historyserver:redhat_ubi8-hadoop3.2.1-java8 docker/hadoop/historyserver

docker build -t barryma22/hive:2.3.2-postgresql-metastore docker/hive/base
docker build -t barryma22/hive-metastore-postgresql:2.3.0 docker/hive/metastore
docker build -t barryma22/spark-base:redhat_ubi8 docker/spark/base
docker build -t barryma22/spark-master:redhat_ubi8-hadoop3 docker/spark/master
docker build -t barryma22/spark-worker:redhat_ubi8-hadoop3 docker/spark/worker
docker build -t barryma22/spark-history-server:redhat_ubi8-hadoop3 docker/spark/history-server