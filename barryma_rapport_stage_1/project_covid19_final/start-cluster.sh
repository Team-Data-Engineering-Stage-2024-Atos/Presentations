#!/bin/bash

LOG_FILE="start-cluster.log"
ERROR_LOG_FILE="start-cluster-error.log"

log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

error_log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') - ERROR: $1" | tee -a "$ERROR_LOG_FILE" >&2
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

check_service_status() {
    local service_name=$1
    local container_name=$2
    if docker exec "$container_name" curl -s -o /dev/null -w "%{http_code}" "http://localhost:${service_name}" | grep -q "200"; then
        log "$service_name is running."
    else
        error_log "$service_name is not running."
        exit 1
    fi
}

send_email_notification() {
    local subject=$1
    local message=$2
    local recipient="mxmbx22@gmail.com"
    echo "$message" | mail -s "$subject" "$recipient"
}

main() {
    log "Launching cluster...."

    if docker-compose up -d; then
        log "Cluster launched successfully."
        log "Configuration of the cluster...."
        sleep 10
    else
        error_log "Error launching cluster."
        send_email_notification "Cluster Launch Failed" "The cluster failed to launch. Check the error logs for details."
        exit 1
    fi

    local container_name="cov19-namenode"
    if check_namenode "$container_name"; then
        log "NameNode is available."

        local directories=("/user/spark/spark-logs" "/user/spark/jars")

        for directory in "${directories[@]}"; do
            create_hdfs_directory "$container_name" "$directory"
        done

        put_hdfs_files "$container_name" "/opt/spark/jars/*" "/user/spark/jars"

        for directory in "${directories[@]}"; do
            change_hdfs_permissions "$container_name" "$directory" "777"
        done

        log "Checking service status...."
        check_service_status "8088" "$container_name"  # YARN ResourceManager
        check_service_status "4040" "$container_name"  # Spark Web UI
        check_service_status "10000" "$container_name" # HiveServer2

        log "Everything went well!"
        send_email_notification "Cluster Setup Successful" "The cluster was set up successfully."
    else
        error_log "NameNode is not available. Please check the NameNode status."
        send_email_notification "Cluster Setup Failed" "NameNode is not available. Please check the NameNode status."
        exit 1
    fi
}

main
