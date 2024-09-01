#!/bin/bash

export PATH=$PATH:sbin:$SBT_HOME/bin:$SCALA_HOME/bin:$JAVA_HOME/bin:$NIFI_HOME/bin

# Start SSH service
sudo /usr/sbin/sshd
if [ $? -ne 0 ]; then
    echo 'Failed to start SSH service'
    exit 1
fi

# Generate SSH key for hadoopuser and distribute it
if [ ! -f /home/hadoopuser/.ssh/id_rsa ]; then
    ssh-keygen -t rsa -b 2048 -f /home/hadoopuser/.ssh/id_rsa -q -N ''
fi


# Nifi
$NIFI_HOME/bin/nifi.sh start
if [ $? -ne 0 ]; then
    echo 'Failed to start Nifi'
    exit 1
fi
tail -f $NIFI_HOME/logs/nifi-app.log


# Keep the container running
tail -f /dev/null
