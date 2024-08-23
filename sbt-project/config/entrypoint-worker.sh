#!/bin/bash

export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$SBT_HOME/bin:$SCALA_HOME/bin:$JAVA_HOME/bin

# Start SSH service
sudo /usr/sbin/sshd
if [ $? -ne 0 ]; then
    echo 'Failed to start SSH service'
    exit 1
fi

sudo chown -R hadoopuser:hadoopuser /var/hdfs/data && sudo chmod -R 755 /var/hdfs/data # mkdir -p /var/hdfs/data && 

# Start HDFS DataNode directly
hdfs --daemon start datanode
if [ $? -ne 0 ]; then
    echo 'Failed to start HDFS DataNode'
    exit 1
fi

# Start YARN NodeManager directly
yarn --daemon start nodemanager
if [ $? -ne 0 ]; then
    echo 'Failed to start YARN NodeManager'
    exit 1
fi

# Start Spark Worker
$SPARK_HOME/sbin/start-slave.sh spark://master-namenode:7077
if [ $? -ne 0 ]; then
    echo 'Failed to start Spark Worker'
    exit 1
fi

# Keep the container running
tail -f /dev/null
