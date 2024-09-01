#!/bin/bash

# Start the YARN NodeManager
if [ -x "$HADOOP_HOME/bin/yarn" ]; then
    $HADOOP_HOME/bin/yarn --config $HADOOP_CONF_DIR nodemanager
else
    echo "Error: YARN binary not found or not executable at $HADOOP_HOME/bin/yarn"
    exit 1
fi
