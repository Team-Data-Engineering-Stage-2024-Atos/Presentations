#!/bin/bash

# Define an associative array with key-value pairs
declare -A datasets
datasets=(
    ["/user/hadoop/cov19_data/raw"]="/opt/data/sample/RAW_global_confirmed_cases_sample.csv /opt/data/sample/RAW_global_deaths_sample.csv"
    ["/user/hadoop/cov19_data/us_raw"]="/opt/data/sample/RAW_us_confirmed_cases_sample.csv /opt/data/sample/RAW_us_deaths_sample.csv"
)

# Iterate over the associative array
for hdfs_dest in "${!datasets[@]}"; do
    # Check if the directory exists
    if ! hdfs dfs -test -e $hdfs_dest; then
        # If not, create the directory
        hdfs dfs -mkdir -p $hdfs_dest
    fi

    # Split the string into an array
    IFS=' ' read -r -a local_paths <<< "${datasets[$hdfs_dest]}"

    # Iterate over the local paths
    for local_path in "${local_paths[@]}"; do
        # Upload the file to HDFS
        hdfs dfs -put $local_path $hdfs_dest
    done
done
