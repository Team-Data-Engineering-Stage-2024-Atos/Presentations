FROM barryma22/hadoop-base:redhat_ubi8

# Set up HDFS environment variables and directories
ENV HDFS_CONF_dfs_namenode_name_dir=file:///hadoop/dfs/name
RUN mkdir -p /hadoop/dfs/name
VOLUME /hadoop/dfs/name

# Add the run script
ADD run.sh /run.sh
RUN chmod a+x /run.sh

# Expose the necessary port
EXPOSE 9870

# Healthcheck command to ensure the service is running
HEALTHCHECK CMD curl -f http://localhost:9870/ || exit 1

# Set the default command to execute the run script
CMD ["/run.sh"]
