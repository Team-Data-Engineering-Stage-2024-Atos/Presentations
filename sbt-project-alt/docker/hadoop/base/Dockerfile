FROM redhat/ubi8:8.10

# Install necessary packages
RUN yum -y update && \
    yum -y install \
      java-1.8.0-openjdk-devel \
      net-tools \
      curl \
      nc \
      perl \
      gnupg2 \
    && yum clean all

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk/

# Download and import Apache Hadoop GPG keys
RUN curl -O https://dist.apache.org/repos/dist/release/hadoop/common/KEYS && \
    gpg --import KEYS

# Set Hadoop version and download URL
ENV HADOOP_VERSION 3.2.1
ENV HADOOP_URL https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz

# Download, verify, and extract Hadoop
RUN set -x && \
    curl -fSL "$HADOOP_URL" -o /tmp/hadoop.tar.gz && \
    curl -fSL "$HADOOP_URL.asc" -o /tmp/hadoop.tar.gz.asc && \
    gpg --verify /tmp/hadoop.tar.gz.asc && \
    tar -xvf /tmp/hadoop.tar.gz -C /opt/ && \
    rm /tmp/hadoop.tar.gz*

# Create symbolic link for Hadoop configuration
RUN ln -s /opt/hadoop-$HADOOP_VERSION/etc/hadoop /etc/hadoop

# Create directories for Hadoop logs and data
RUN mkdir /opt/hadoop-$HADOOP_VERSION/logs && \
    mkdir /hadoop-data

# Set environment variables for Hadoop
ENV HADOOP_HOME=/opt/hadoop-$HADOOP_VERSION
ENV HADOOP_CONF_DIR=/etc/hadoop
ENV MULTIHOMED_NETWORK=1
ENV USER=root
ENV PATH=$HADOOP_HOME/bin/:$PATH

# Add the entrypoint script and make it executable
ADD entrypoint.sh /entrypoint.sh
RUN chmod a+x /entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["/entrypoint.sh"]
