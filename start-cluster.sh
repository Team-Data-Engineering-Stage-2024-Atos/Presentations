#!/bin/bash

docker-compose up -d
echo "LANCEMENT DU CLUSTER......." && sleep 120

echo "STOCKAGE DES DONNEES......."
docker exec namenode hdfs dfs -mkdir /data
docker exec namenode hdfs dfs -put /opt/data/demo.csv /data/

echo "BIENVENUE"
sed -n 13,32p README.md