FROM barryma22/hadoop-base:redhat_ubi8

# Allow build-time config of HIVE_VERSION
ARG HIVE_VERSION
# Set HIVE_VERSION from arg if provided at build, env if provided at run, or default
ENV HIVE_VERSION=${HIVE_VERSION:-2.3.2}

# Set environment variables for Hive and Hadoop
ENV HIVE_HOME /opt/hive
ENV HADOOP_HOME /opt/hadoop-$HADOOP_VERSION
ENV PATH $HIVE_HOME/bin:$HADOOP_HOME/bin:$PATH

WORKDIR /opt

# Install necessary packages, Hive, and PostgreSQL JDBC driver
RUN yum -y update && yum install -y wget procps-ng nc perl sed hostname && \
    wget https://archive.apache.org/dist/hive/hive-$HIVE_VERSION/apache-hive-$HIVE_VERSION-bin.tar.gz && \
    tar -xzvf apache-hive-$HIVE_VERSION-bin.tar.gz && \
    mv apache-hive-$HIVE_VERSION-bin hive && \
    wget https://jdbc.postgresql.org/download/postgresql-42.3.1.jar -O $HIVE_HOME/lib/postgresql-jdbc.jar && \
    rm -f $HIVE_HOME/lib/guava-*.jar && \
    wget https://repo1.maven.org/maven2/com/google/guava/guava/14.0.1/guava-14.0.1.jar -O $HIVE_HOME/lib/guava.jar && \
    rm apache-hive-$HIVE_VERSION-bin.tar.gz && \
    yum remove -y wget && \
    yum clean all && \
    rm -rf /var/cache/yum

# Add custom Hive configurations
ADD conf/hive-site.xml $HIVE_HOME/conf
ADD conf/beeline-log4j2.properties $HIVE_HOME/conf
ADD conf/hive-env.sh $HIVE_HOME/conf
ADD conf/hive-exec-log4j2.properties $HIVE_HOME/conf
ADD conf/hive-log4j2.properties $HIVE_HOME/conf
ADD conf/ivysettings.xml $HIVE_HOME/conf
ADD conf/llap-daemon-log4j2.properties $HIVE_HOME/conf

# Copy and make startup and entrypoint scripts executable
COPY startup.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/startup.sh

COPY entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/entrypoint.sh

# Expose necessary ports
EXPOSE 10000
EXPOSE 10002

# Set the entrypoint and command
ENTRYPOINT ["entrypoint.sh"]
CMD ["startup.sh"]
