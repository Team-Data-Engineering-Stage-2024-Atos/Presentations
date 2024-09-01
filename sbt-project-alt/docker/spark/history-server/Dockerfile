FROM barryma22/spark-base:redhat_ubi8

# Copy the history server script
COPY history-server.sh /history-server.sh

# Expose the necessary port for the Spark History Server
EXPOSE 18081

# Set the command to start the Spark History Server
CMD ["/bin/bash", "/history-server.sh"]
