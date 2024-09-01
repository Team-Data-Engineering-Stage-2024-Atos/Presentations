FROM redhat/ubi8:8.10

ARG SBT_VERSION=1.8.3

# Dependencies
RUN yum install -y java-1.8.0-openjdk-devel wget sudo \
    procps-ng hostname openssh-server openssh-clients sshpass && \
    yum clean all

# Tools

RUN wget https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz && \
    tar -xvzf sbt-${SBT_VERSION}.tgz && \
    mv sbt /usr/local/sbt && \
    rm sbt-${SBT_VERSION}.tgz

RUN yum install unzip -y && yum clean all

RUN wget https://dlcdn.apache.org/nifi/1.27.0/nifi-1.27.0-bin.zip && \
    unzip nifi-1.27.0-bin.zip && \
    mv nifi-1.27.0 /usr/local/nifi-1.27.0 && \
    rm nifi-1.27.0-bin.zip

RUN wget https://dlcdn.apache.org/nifi/1.27.0/nifi-toolkit-1.27.0-bin.zip && \
    unzip nifi-toolkit-1.27.0-bin.zip  && \
    mv nifi-toolkit-1.27.0 /usr/local/nifi-toolkit-1.27.0 && \
    rm nifi-toolkit-1.27.0-bin.zip


ENV SPARK_VERSION=3.3.0
ENV HADOOP_VERSION=3

RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar xvf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV SPARK_MASTER_URL=spark://spark-master:7077
# Environment
ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk \
    SBT_HOME=/usr/local/sbt \
    SCALA_HOME=/usr/local/scala \
    NIFI_HOME=/usr/local/nifi-1.27.0 \
    PATH=$PATH:$SBT_HOME/bin:$SCALA_HOME/bin:$JAVA_HOME/bin:$NIFI_HOME/bin

# RUN wget https://jdbc.postgresql.org/download/postgresql-42.3.1.jar -O $SPARK_HOME/jars/postgresql-42.3.1.jar 
RUN wget https://repo1.maven.org/maven2/org/apache/nifi/nifi-spark-receiver/1.27.0/nifi-spark-receiver-1.27.0.jar -O $SPARK_HOME/jars/nifi-spark-receiver-1.27.0.jar
RUN wget https://repo1.maven.org/maven2/org/apache/nifi/nifi-site-to-site-client/1.27.0/nifi-site-to-site-client-1.27.0.jar -O $SPARK_HOME/jars/nifi-site-to-site-client-1.27.0.jar

RUN useradd -ms /bin/bash hadoopuser && \
    echo "hadoopuser:passer" | chpasswd && usermod -aG wheel hadoopuser && \
    sed -i 's/^# %wheel/%wheel/' /etc/sudoers

RUN echo 'hadoopuser ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

# SSH configuration
RUN mkdir /var/run/sshd && mkdir -p /home/hadoopuser/.ssh/ && \
    ssh-keygen -t rsa -b 2048 -f /etc/ssh/ssh_host_rsa_key -N '' && \
    ssh-keygen -t ecdsa -b 521 -f /etc/ssh/ssh_host_ecdsa_key -N '' && \
    ssh-keygen -t ed25519 -f /etc/ssh/ssh_host_ed25519_key -N '' && \
    ssh-keygen -t rsa -N "" -f /home/hadoopuser/.ssh/id_rsa && \
    cat /home/hadoopuser/.ssh/id_rsa.pub >> /home/hadoopuser/.ssh/authorized_keys && \
    chmod 600 /home/hadoopuser/.ssh/authorized_keys && \
    chown -R hadoopuser:hadoopuser /home/hadoopuser/.ssh

# Ensure the sshd service runs
RUN sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config

RUN echo "export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk" >> /home/hadoopuser/.bashrc && \
    echo "export SBT_HOME=/usr/local/sbt" >> /home/hadoopuser/.bashrc && \
    echo "export SCALA_HOME=/usr/local/scala" >> /home/hadoopuser/.bashrc && \
    echo "export NIFI_HOME=/usr/local/nifi-1.27.0/" >> /home/hadoopuser/.bashrc && \
    echo 'export PATH=$PATH:$SBT_HOME/bin:$SCALA_HOME/bin:$JAVA_HOME/bin:$NIFI_HOME/bin' >> /home/hadoopuser/.bashrc

# Finalization

WORKDIR /app

COPY nifi/conf/* $NIFI_HOME/conf/

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

EXPOSE 22 8090

USER root
RUN chmod 600 /etc/ssh/sshd_config && chown -R hadoopuser:hadoopuser /usr/local/nifi-1.27.0/ && chmod -R 755 /usr/local/nifi-1.27.0/

USER hadoopuser

CMD [ "/usr/local/bin/entrypoint.sh" ]
