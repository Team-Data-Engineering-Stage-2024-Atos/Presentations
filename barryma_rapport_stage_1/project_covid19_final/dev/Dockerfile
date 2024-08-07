# Use the Python slim image
FROM python:3.8-slim

# Install dependencies
RUN apt-get update && \
    apt-get install -y wget openjdk-8-jdk curl && \
    rm -rf /var/lib/apt/lists/*

# Set up Hadoop environment variables
ENV HADOOP_VERSION=3.2.1
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin

# Install Hadoop
RUN wget https://archive.apache.org/dist/hadoop/core/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz && \
    tar -xzvf hadoop-$HADOOP_VERSION.tar.gz -C /opt/ && \
    ln -s /opt/hadoop-$HADOOP_VERSION /opt/hadoop && \
    rm hadoop-$HADOOP_VERSION.tar.gz

# Install Python dependencies
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ src/
COPY conf/ conf/
COPY datasets/ datasets/
COPY main.py main.py

CMD ["python", "main.py"]
