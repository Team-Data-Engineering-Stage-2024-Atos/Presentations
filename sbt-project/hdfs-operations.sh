#!/bin/bash

source ~/.bashrc

set -e


HDFS_BASE_PATH="/user/hadoopuser"
HDFS_DATASETS_PATH="$HDFS_BASE_PATH/datasets"
HDFS_SPARK_JARS_PATH="$HDFS_BASE_PATH/spark-jars"
HDFS_SPARK_LOGS_PATH="$HDFS_BASE_PATH/spark-logs"
LOG_DIR="/home/hadoopuser/logs"
LOG_FILE="${LOG_DIR}/hdfs-operations.log"


mkdir -p $LOG_DIR


function check_command {
    if [ $? -ne 0 ]; then
        echo "Error during: $1" | tee -a $LOG_FILE
        exit 1
    fi
}


function upload_datasets {
    echo "Uploading datasets to HDFS..." | tee -a $LOG_FILE
    for dataset in /datasets/*; do
        hdfs dfs -put $dataset $HDFS_DATASETS_PATH/ >> $LOG_FILE 2>&1
        check_command "put $dataset to HDFS"
    done
    echo "Datasets uploaded successfully." | tee -a $LOG_FILE
}


function cleanup_datasets {
    echo "Cleaning up datasets in HDFS..." | tee -a $LOG_FILE
    hdfs dfs -rm -r $HDFS_DATASETS_PATH >> $LOG_FILE 2>&1
    check_command "delete datasets directory in HDFS"
    echo "Datasets cleaned up successfully." | tee -a $LOG_FILE
}


function setup_spark_directories {
    echo "Setting up Spark directories in HDFS..." | tee -a $LOG_FILE
    hdfs dfs -mkdir -p $HDFS_SPARK_JARS_PATH >> $LOG_FILE 2>&1
    check_command "create spark-jars directory"

    hdfs dfs -mkdir -p $HDFS_SPARK_LOGS_PATH >> $LOG_FILE 2>&1
    check_command "create spark-logs directory"

    hdfs dfs -chmod 775 $HDFS_SPARK_LOGS_PATH >> $LOG_FILE 2>&1
    check_command "chmod on spark-logs directory"

    hdfs dfs -chmod 775 $HDFS_SPARK_JARS_PATH >> $LOG_FILE 2>&1
    check_command "chmod on spark-jars directory"
    echo "Spark directories set up successfully." | tee -a $LOG_FILE
}

function upload_spark_jars {
    echo "Uploading Spark JARs to HDFS..." | tee -a $LOG_FILE
    hdfs dfs -put $SPARK_HOME/jars/*.jar $HDFS_SPARK_JARS_PATH/ >> $LOG_FILE 2>&1
    check_command "put Spark jars to HDFS"
    echo "Spark JARs uploaded successfully." | tee -a $LOG_FILE
}


function initial_setup() {
    echo "Performing initial setup..." | tee -a $LOG_FILE
    setup_spark_directories
    hdfs dfs -mkdir -p /user/hadoopuser/datasets
    hdfs dfs -chmod 775 /user/hadoopuser/datasets
    upload_datasets
    upload_spark_jars
    echo "Initial setup completed successfully." | tee -a $LOG_FILE
}


function show_hdfs_menu() {
    while true; do
        printf "\033c"
        echo "HDFS Operations Menu"
        echo "--------------------"
        echo "1. Initial Setup"
        echo "2. Upload Datasets to HDFS"
        echo "3. Cleanup Datasets in HDFS"
        echo "4. Setup Spark Directories in HDFS"
        echo "5. Upload Spark JARs to HDFS"
        echo "6. Exit to Shell"
        echo "--------------------"
        read -p "Enter choice [1-6]: " choice
        case $choice in
            1) initial_setup ;;
            2) upload_datasets ;;
            3) cleanup_datasets ;;
            4) setup_spark_directories ;;
            5) upload_spark_jars ;;
            6) break ;;
            *) echo "Invalid choice! Please select a valid option." ;;
        esac
        echo "Press any key to return to the HDFS menu..."
        read -n 1
    done
}

case $1 in
    menu) show_hdfs_menu ;;
    upload_datasets) upload_datasets ;;
    cleanup_datasets) cleanup_datasets ;;
    setup_spark_directories) setup_spark_directories ;;
    upload_spark_jars) upload_spark_jars ;;
    *) echo "Invalid command. Use 'menu' for HDFS operations menu or one of: initial_setup, upload_datasets, cleanup_datasets, setup_spark_directories, upload_spark_jars" ;;
esac
