#!/bin/bash

# Remove 'file://' prefix from the namenode directory path
namedir=$(echo $HDFS_CONF_dfs_namenode_name_dir | perl -pe 's#file://##')
if [ ! -d "$namedir" ]; then
  echo "Namenode name directory not found: $namedir"
  exit 2
fi

# Check if the cluster name is provided
if [ -z "$CLUSTER_NAME" ]; then
  echo "Cluster name not specified"
  exit 2
fi

# Remove the 'lost+found' directory if it exists
echo "Removing lost+found from $namedir"
rm -rf "$namedir/lost+found"

# Format the namenode if the directory is empty
if [ -z "$(ls -A $namedir)" ]; then
  echo "Formatting namenode name directory: $namedir"
  $HADOOP_HOME/bin/hdfs --config $HADOOP_CONF_DIR namenode -format $CLUSTER_NAME
fi

# Start the namenode
$HADOOP_HOME/bin/hdfs --config $HADOOP_CONF_DIR namenode
