#!/bin/bash

# Set the Spark Master host, defaulting to the hostname if not provided
export SPARK_MASTER_HOST=${SPARK_MASTER_HOST:-$(hostname)}

# Set the Spark home directory
export SPARK_HOME=/spark

# Source Spark configuration and environment variables
. "$SPARK_HOME/sbin/spark-config.sh"
. "$SPARK_HOME/bin/load-spark-env.sh"

# Ensure the Spark Master log directory exists
mkdir -p $SPARK_MASTER_LOG

# Redirect stdout to the Spark Master log file
ln -sf /dev/stdout $SPARK_MASTER_LOG/spark-master.out

# Start the Spark Master with the specified IP, port, and web UI port
cd $SPARK_HOME/bin && $SPARK_HOME/sbin/../bin/spark-class org.apache.spark.deploy.master.Master \
    --ip $SPARK_MASTER_HOST --port $SPARK_MASTER_PORT --webui-port $SPARK_MASTER_WEBUI_PORT >> $SPARK_MASTER_LOG/spark-master.out
