#!/bin/bash

# Set the Spark home directory
export SPARK_HOME=/spark

# Source Spark configurations
. "$SPARK_HOME/sbin/spark-config.sh"

# Load Spark environment variables
. "$SPARK_HOME/bin/load-spark-env.sh"

# Set up the Spark History Server log directory
SPARK_HS_LOG_DIR=$SPARK_HOME/spark-hs-logs
mkdir -p $SPARK_HS_LOG_DIR

# Define the log file and link stdout to it
LOG=$SPARK_HS_LOG_DIR/spark-hs.out
ln -sf /dev/stdout $LOG

# Configure Spark History Server options
# For more details, refer to: https://spark.apache.org/docs/latest/monitoring.html#environment-variables
export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=/tmp/spark-events -Dspark.history.ui.port=18081"

# Start the Spark History Server and redirect logs to stdout
cd $SPARK_HOME/bin && $SPARK_HOME/sbin/../bin/spark-class org.apache.spark.deploy.history.HistoryServer >> $LOG
