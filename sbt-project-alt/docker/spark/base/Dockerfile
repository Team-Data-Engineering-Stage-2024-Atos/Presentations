FROM redhat/ubi8:8.10

# Environment variables for initialization and Spark
ENV ENABLE_INIT_DAEMON false
ENV INIT_DAEMON_BASE_URI http://identifier/init-daemon
ENV INIT_DAEMON_STEP spark_master_init

ENV BASE_URL=https://archive.apache.org/dist/spark/
ENV SPARK_VERSION=3.3.0
ENV HADOOP_VERSION=3

# Copy necessary scripts to the container
COPY wait-for-step.sh /
COPY execute-step.sh /
COPY finish-step.sh /

# Install necessary packages
RUN yum -y update && \
    yum -y install curl wget bash java-1.8.0-openjdk python3 python3-pip nss procps-ng hostname && \
    ln -s /usr/lib64/ld-linux-x86-64.so.2 /lib/ld-linux-x86-64.so.2 && \
    chmod +x *.sh && \
    wget ${BASE_URL}/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    cd /

# Grant execute permissions to the copied scripts
RUN chmod +x /wait-for-step.sh && chmod +x /execute-step.sh && chmod +x /finish-step.sh

# COPY hive-site.xml /spark/conf/hive-site.xml

# Fix the value of PYTHONHASHSEED
# Note: this is needed when you use Python 3.3 or greater
ENV PYTHONHASHSEED 1
