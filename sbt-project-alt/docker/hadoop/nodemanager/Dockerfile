FROM barryma22/hadoop-base:redhat_ubi8

# Add the run script
ADD run.sh /run.sh
RUN chmod a+x /run.sh

# Expose the necessary port
EXPOSE 8042

# Healthcheck command to ensure the service is running
HEALTHCHECK CMD curl -f http://localhost:8042/ || exit 1

# Set the default command to execute the run script
CMD ["/run.sh"]
