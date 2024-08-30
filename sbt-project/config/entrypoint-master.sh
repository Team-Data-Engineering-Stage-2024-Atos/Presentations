#!/bin/bash

export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$SBT_HOME/bin:$SCALA_HOME/bin:$JAVA_HOME/bin:$NIFI_HOME/bin

# Start SSH service
sudo /usr/sbin/sshd
if [ $? -ne 0 ]; then
    echo 'Failed to start SSH service'
    exit 1
fi

# Generate SSH key for hadoopuser and distribute it
if [ ! -f /home/hadoopuser/.ssh/id_rsa ]; then
    ssh-keygen -t rsa -b 2048 -f /home/hadoopuser/.ssh/id_rsa -q -N ''
    sshpass -p 'passer' ssh-copy-id -o StrictHostKeyChecking=no hadoopuser@worker-datanode1
    sshpass -p 'passer' ssh-copy-id -o StrictHostKeyChecking=no hadoopuser@worker-datanode2
fi

hdfs --daemon stop namenode

# Format NameNode if needed
if [ ! -d "/var/hdfs/name/current" ]; then
    echo "Formatting NameNode..."
    sudo chown -R hadoopuser:hadoopuser /var/hdfs/name && sudo chmod -R 755 /var/hdfs/name #mkdir -p /var/hdfs/name && 
    
    hdfs namenode -format -force -nonInteractive
    if [ $? -ne 0 ]; then
        echo "Failed to format NameNode. Exiting..."
        exit 1
    fi
else
    echo "NameNode already formatted."
fi

# Start HDFS Namenode
hdfs --daemon start namenode
if [ $? -ne 0 ]; then
    echo 'Failed to start HDFS Namenode'
    exit 1
fi

# Start YARN ResourceManager
yarn --daemon start resourcemanager
if [ $? -ne 0 ]; then
    echo 'Failed to start YARN ResourceManager'
    exit 1
fi

# Start Spark Master
$SPARK_HOME/sbin/start-master.sh
if [ $? -ne 0 ]; then
    echo 'Failed to start Spark Master'
    exit 1
fi

echo "Setting up the cluster, please wait..." && sleep 30

hdfs dfs -mkdir -p /user/hadoopuser/spark-jars
hdfs dfs -mkdir -p /user/hadoopuser/spark-logs
hdfs dfs -mkdir -p /user/hadoopuser/sparkstages

hdfs dfs -chmod 775 /user/hadoopuser/spark-logs
hdfs dfs -chmod 775 /user/hadoopuser/spark-jars
hdfs dfs -chmod 775 /user/hadoopuser/sparkstages

hdfs dfs -put $SPARK_HOME/jars/*.jar /user/hadoopuser/spark-jars/


$SPARK_HOME/sbin/start-history-server.sh
if [ $? -ne 0 ]; then
    echo 'Failed to start Spark History Server'
    exit 1
fi


# Nifi
$NIFI_HOME/bin/nifi.sh start
if [ $? -ne 0 ]; then
    echo 'Failed to start Nifi'
    exit 1
fi
#tail -f $NIFI_HOME/logs/nifi-app.log


# Keep the container running
tail -f /dev/null
