FROM barryma22/spark-base:redhat_ubi8

# Set environment variables for Spark Worker
ENV SPARK_WORKER_WEBUI_PORT 8081
ENV SPARK_WORKER_LOG /spark/logs
ENV SPARK_MASTER "spark://spark-master:7077"

# Create directories for logs
RUN mkdir -p $SPARK_WORKER_LOG

# Copy the worker.sh script
COPY worker.sh /worker.sh

# Expose the necessary port for Spark Worker
EXPOSE 8081

# Set the command to run the Spark Worker
CMD ["/bin/bash", "/worker.sh"]
