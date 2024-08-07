#!/bin/bash

#/spark/bin/spark-submit --master yarn /opt/scripts/python/BasicStatistics.py
#spark://spark-master:7077  

#/spark/bin/spark-submit --master yarn --deploy-mode cluster --class org.apache.spark.examples.SparkPi /path/to/examples.jar 1000


# /spark//bin/spark-submit \
# --deploy-mode cluster \
# --class <main_class> \
# --master local \

/spark/bin/spark-submit \
--master local /opt/scripts/python/BasicStatistics.py