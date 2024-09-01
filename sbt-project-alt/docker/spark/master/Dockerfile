FROM barryma22/spark-base:redhat_ubi8

# Set environment variables for Spark Master
ENV SPARK_MASTER_PORT 7077
ENV SPARK_MASTER_WEBUI_PORT 8080
ENV SPARK_MASTER_LOG /spark/logs

# Create directories for logs
RUN mkdir -p $SPARK_MASTER_LOG

# Copy the master.sh script
COPY master.sh /master.sh

# Expose necessary ports
EXPOSE 8080 7077 6066

# Set the command to run the Spark Master
CMD ["/bin/bash", "/master.sh"]
