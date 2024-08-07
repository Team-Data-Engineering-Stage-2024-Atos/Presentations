#!/bin/bash

LOG_FILE="stop-cluster.log"
ERROR_LOG_FILE="stop-cluster-error.log"
EMAIL_RECIPIENT="mxmbx22@gmail.com"

log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

error_log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') - ERROR: $1" | tee -a "$ERROR_LOG_FILE" >&2
}

send_email_notification() {
    local subject=$1
    local message=$2
    echo "$message" | mail -s "$subject" "$EMAIL_RECIPIENT"
}

stop_docker_containers() {
    if docker-compose down; then
        log "Docker containers stopped successfully."
    else
        error_log "Error stopping Docker containers."
        send_email_notification "Cluster Shutdown Failed" "The cluster failed to shut down. Check the error logs for details."
        exit 1
    fi
}

main() {
    log "Stopping cluster...."

    stop_docker_containers

    log "Cluster stopped successfully."
    send_email_notification "Cluster Shutdown Successful" "The cluster was shut down successfully."
}

main
