#!/bin/bash

LOG_FILE="manage-cluster.log"
ERROR_LOG_FILE="manage-cluster-error.log"
EMAIL_RECIPIENT="mxmbx22@gmail.com"
BACKUP_DIR="/backup"
LOG_DIR="/var/log"

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

check_service_status() {
    local service_name=$1
    local container_name=$2
    local port=$3
    local status_code=$(docker exec "$container_name" curl -s -o /dev/null -w "%{http_code}" "http://localhost:${port}")
    if [ "$status_code" -eq 200 ]; then
        log "$service_name is running."
    else
        error_log "$service_name is not running. Status code: $status_code"
    fi
}

check_hdfs_directory() {
    local container_name=$1
    local directory=$2
    docker exec "$container_name" hdfs dfs -test -d "$directory"
    return $?
}

check_namenode() {
    local container_name=$1
    local namenode_url="http://localhost:9870"
    local status_code=$(docker exec "$container_name" curl -s -o /dev/null -w "%{http_code}" "$namenode_url")
    if [ "$status_code" -eq 200 ]; then
        return 0
    else
        return 1
    fi
}

create_hdfs_directory() {
    local container_name=$1
    local directory=$2
    if ! check_hdfs_directory "$container_name" "$directory"; then
        if docker exec "$container_name" hdfs dfs -mkdir -p "$directory"; then
            log "Directory $directory created successfully."
        else
            error_log "Error creating directory $directory."
            exit 1
        fi
    else
        log "Directory $directory already exists."
    fi
}

change_hdfs_permissions() {
    local container_name=$1
    local directory=$2
    local permissions=$3
    if docker exec "$container_name" hdfs dfs -chmod "$permissions" "$directory"; then
        log "Permissions for $directory set to $permissions."
    else
        error_log "Error setting permissions for directory $directory."
        exit 1
    fi
}

put_hdfs_files() {
    local container_name=$1
    local local_path=$2
    local hdfs_path=$3
    if docker exec "$container_name" hdfs dfs -put "$local_path" "$hdfs_path"; then
        log "Files from $local_path uploaded to $hdfs_path."
    else
        error_log "Error uploading files from $local_path to $hdfs_path."
        exit 1
    fi
}

start_cluster() {
    log "Launching cluster...."
    if docker-compose up -d; then
        log "Cluster launched successfully."
        log "Configuration of the cluster...."
        sleep 10

        local container_name="cov19-namenode"
        if check_namenode "$container_name"; then
            log "NameNode is available."

            local directories=("/user/spark/spark-logs" "/user/spark/jars")
            for directory in "${directories[@]}"; do
                create_hdfs_directory "$container_name" "$directory"
            done

            put_hdfs_files "$container_name" "/opt/spark/jars/" "/user/spark/jars"
            for directory in "${directories[@]}"; do
                change_hdfs_permissions "$container_name" "$directory" "777"
            done

            log "Checking service status...."
            check_service_status "YARN ResourceManager" "$container_name" "8088"
            check_service_status "Spark Web UI" "$container_name" "4040"
            check_service_status "HiveServer2" "$container_name" "10000"

            log "Everything went well!"
            send_email_notification "Cluster Setup Successful" "The cluster was set up successfully."
        else
            error_log "NameNode is not available. Please check the NameNode status."
            send_email_notification "Cluster Setup Failed" "NameNode is not available. Please check the NameNode status."
            exit 1
        fi
    else
        error_log "Error launching cluster."
        send_email_notification "Cluster Launch Failed" "The cluster failed to launch. Check the error logs for details."
        exit 1
    fi
}

stop_cluster() {
    log "Stopping cluster...."
    if docker-compose down; then
        log "Cluster stopped successfully."
        send_email_notification "Cluster Shutdown Successful" "The cluster was shut down successfully."
    else
        error_log "Error stopping cluster."
        send_email_notification "Cluster Shutdown Failed" "The cluster failed to shut down. Check the error logs for details."
        exit 1
    fi
}

restart_cluster() {
    stop_cluster
    start_cluster
}

status_cluster() {
    log "Checking cluster status...."
    local container_name="cov19-namenode"
    check_service_status "NameNode" "$container_name" "9870"
    check_service_status "YARN ResourceManager" "$container_name" "8088"
    check_service_status "Spark Web UI" "$container_name" "4040"
    check_service_status "HiveServer2" "$container_name" "10000"
    log "Cluster status check completed."
}

backup_data() {
    log "Starting data backup...."
    local container_name="cov19-namenode"
    if docker exec "$container_name" hdfs dfs -mkdir -p "$BACKUP_DIR"; then
        log "Created backup directory $BACKUP_DIR on HDFS."
    else
        error_log "Error creating backup directory $BACKUP_DIR on HDFS."
        exit 1
    fi

    if docker exec "$container_name" hdfs dfs -copyToLocal /user/spark "$BACKUP_DIR"; then
        log "Backup of /user/spark completed successfully."
    else
        error_log "Error backing up /user/spark to $BACKUP_DIR."
        exit 1
    fi
}

cleanup_logs() {
    log "Starting log cleanup...."
    find "$LOG_DIR" -type f -name "*.log" -mtime +7 -exec rm -f {} \;
    log "Old log files cleaned up from $LOG_DIR."
}

setup_monitoring() {
    log "Setting up monitoring services...."
    if docker-compose -f monitoring-compose.yml up -d; then
        log "Monitoring services started successfully."
    else
        error_log "Error starting monitoring services."
        exit 1
    fi
}

show_help() {
    echo "Usage: $0 {start|stop|restart|status|backup|cleanup|monitor|help}"
    echo "Commands:"
    echo "  start     - Start the cluster"
    echo "  stop      - Stop the cluster"
    echo "  restart   - Restart the cluster"
    echo "  status    - Check the status of the cluster"
    echo "  backup    - Backup data from HDFS"
    echo "  cleanup   - Cleanup old log files"
    echo "  monitor   - Setup monitoring services"
    echo "  help      - Show this help message"
}

case "$1" in
    start)
        start_cluster
        ;;
    stop)
        stop_cluster
        ;;
    restart)
        restart_cluster
        ;;
    status)
        status_cluster
        ;;
    backup)
        backup_data
        ;;
    cleanup)
        cleanup_logs
        ;;
    monitor)
        setup_monitoring
        ;;
    help|*)
        show_help
        ;;
esac
