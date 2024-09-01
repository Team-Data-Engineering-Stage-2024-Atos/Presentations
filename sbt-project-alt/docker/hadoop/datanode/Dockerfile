FROM barryma22/hadoop-base:redhat_ubi8

# Set up Hadoop environment variables and directories
ENV HDFS_CONF_dfs_datanode_data_dir=file:///hadoop/dfs/data
RUN mkdir -p /hadoop/dfs/data
VOLUME /hadoop/dfs/data

# Add the run script
ADD run.sh /run.sh
RUN chmod a+x /run.sh

# Expose the necessary port
EXPOSE 9864

# Healthcheck command to ensure the service is running
HEALTHCHECK CMD curl -f http://localhost:9864/ || exit 1

# Set the default command to execute the run script
CMD ["/run.sh"]
