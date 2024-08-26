#!/bin/bash


DOCKER_CONTAINER="master-namenode"
HDFS_SCRIPT="./hdfs-operations.sh"
LOG_DIR="logs"
STARTUP_DELAY=10


mkdir -p $LOG_DIR

function check_command {
    if [ $? -ne 0 ]; then
        echo "Error during: $1" | tee -a $LOG_DIR/hadoop_spark_setup.log
        exit 1
    fi
}

start_cluster() {
    echo "Starting Docker containers..." | tee -a $LOG_DIR/hadoop_spark_setup.log
    docker-compose up -d >> $LOG_DIR/hadoop_spark_setup.log 2>&1
    check_command "docker-compose up"
    echo "Waiting for containers to start..." | tee -a $LOG_DIR/hadoop_spark_setup.log
    sleep $STARTUP_DELAY
    echo "Docker containers started successfully." | tee -a $LOG_DIR/hadoop_spark_setup.log
}


stop_cluster() {
    echo "Stopping Docker containers..." | tee -a $LOG_DIR/hadoop_spark_setup.log
    docker-compose down >> $LOG_DIR/hadoop_spark_setup.log 2>&1
    check_command "docker-compose down"
    echo "Docker containers stopped successfully." | tee -a $LOG_DIR/hadoop_spark_setup.log
}

restart_cluster() {
    stop_cluster
    start_cluster
}

scale_cluster() {
    local workers=${1:-2}
    echo "Scaling the cluster to $workers worker nodes..." | tee -a $LOG_DIR/hadoop_spark_setup.log
    docker-compose up -d --scale worker=$workers >> $LOG_DIR/hadoop_spark_setup.log 2>&1
    check_command "docker-compose scale"
    echo "Cluster scaled successfully to $workers worker nodes." | tee -a $LOG_DIR/hadoop_spark_setup.log
}


copy_hdfs_script() {
    echo "Copying HDFS operations script to container..." | tee -a $LOG_DIR/hadoop_spark_setup.log
    docker cp $HDFS_SCRIPT $DOCKER_CONTAINER:/home/hadoopuser/hdfs-operations.sh >> $LOG_DIR/hadoop_spark_setup.log 2>&1
    check_command "docker cp $HDFS_SCRIPT"
    echo "HDFS operations script copied successfully." | tee -a $LOG_DIR/hadoop_spark_setup.log
}

connect_to_container() {
    echo "Connecting to the container..." | tee -a $LOG_DIR/hadoop_spark_setup.log
    docker exec -it $DOCKER_CONTAINER /bin/bash
}

run_hdfs_operations() {
    copy_hdfs_script
    echo "Running HDFS operations menu inside the container..." | tee -a $LOG_DIR/hadoop_spark_setup.log
    docker exec -it $DOCKER_CONTAINER /bin/bash -c "source ~/.bashrc && bash /home/hadoopuser/hdfs-operations.sh menu"
}

show_main_menu() {
    clear
    echo "Hadoop and Spark Cluster Management"
    echo "-----------------------------------"
    echo "1. Start Cluster"
    echo "2. Stop Cluster"
    echo "3. Restart Cluster"
    echo "4. Reset Cluster"
    echo "5. Scale Cluster"
    echo "6. Run HDFS Operations Menu in Container"
    echo "7. Connect to Container"
    echo "8. Exit"
    echo "-----------------------------------"
}

reset_cluster() {
    echo "Resetting the cluster to default ..." | tee -a $LOG_DIR/hadoop_spark_setup.log
    docker-compose down -v >> $LOG_DIR/hadoop_spark_setup.log 2>&1
    check_command "docker-compose down"
    echo "Reset successfull." | tee -a $LOG_DIR/hadoop_spark_setup.log
}

handle_main_choice() {
    local choice
    read -p "Enter choice [1-8]: " choice
    case $choice in
        1) start_cluster ;;
        2) stop_cluster ;;
        3) restart_cluster ;;
        4)
            reset_cluster ;;
        5) 
            read -p "Enter the number of worker nodes (default: 2): " workers
            scale_cluster $workers
            ;;
        6) run_hdfs_operations ;;
        7) connect_to_container ;;
        8) exit 0 ;;
        *) echo "Invalid choice! Please select a valid option." ;;
    esac
}


while true; do
    show_main_menu
    handle_main_choice
    echo "Press any key to return to the main menu..."
    read -n 1
done
