export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
export SPARK_MASTER_HOST=master-namenode


export SPARK_DAEMON_MEMORY=4g
export SPARK_WORKER_MEMORY=16g
export SPARK_WORKER_CORES=8
export SPARK_EXECUTOR_MEMORY=8g
export SPARK_DRIVER_MEMORY=4g

# JVM options
export SPARK_DAEMON_JAVA_OPTS="-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -Xloggc:/var/log/spark/gc.log"
export SPARK_EXECUTOR_JAVA_OPTS="-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -Xloggc:/var/log/spark/gc.log"
export SPARK_DRIVER_JAVA_OPTS="-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -Xloggc:/var/log/spark/gc.log"

# Network settings
export SPARK_NETWORK_TIMEOUT=600s
export SPARK_RPC_ASKTIMEOUT=300s
export SPARK_RPC_LOOKUP_TIMEOUT=300s
export SPARK_CORE_CONNECTION_ACK_WAIT_TIMEOUT=300s

# YARN settings
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf
