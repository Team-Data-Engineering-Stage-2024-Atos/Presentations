export JAVA_HOME=/usr/lib/jvm/java-1.8-openjdk
export SPARK_LOG_DIR=/var/log/spark

# export HADOOP_CONF_DIR=/opt/hadoop-config
export YARN_CONF_DIR=/opt/hadoop-jars/yarn
# export LD_LIBRARY_PATH=/opt/hadoop-lib/lib/native:$LD_LIBRARY_PATH

# export SPARK_DIST_CLASSPATH=/opt/:$(find /opt/hadoop-jars -name '*.jar' | tr '\n' ':')
# export SPARK_CLASSPATH=$SPARK_DIST_CLASSPATH