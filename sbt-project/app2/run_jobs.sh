#!/bin/bash

config_file="src/main/resources/application.conf"

batch1=("dim_customers" "dim_products")
batch2=("dim_dates" "fact_sales")

log_dir="spark_logs_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$log_dir"

for table_name in "${batch1[@]}"; do
  echo "Running spark-submit for $table_name..."
  log_file="$log_dir/spark_job_${table_name}.log"
  spark-submit \
    --class Main \
    --deploy-mode client \
    target/scala-2.11/app2-assembly-1.0.jar \
    "$config_file" "$table_name" &> "$log_file"

  if [ $? -eq 0 ]; then
    echo "$table_name completed successfully." | tee -a "$log_file"
  else
    echo "$table_name failed." | tee -a "$log_file"
  fi
done

for table_name in "${batch2[@]}"; do
  echo "Running spark-submit for $table_name..."
  log_file="$log_dir/spark_job_${table_name}.log"
  spark-submit \
    --class Main \
    --deploy-mode client \
    target/scala-2.11/app2-assembly-1.0.jar \
    "$config_file" "$table_name" &> "$log_file"

  if [ $? -eq 0 ]; then
    echo "$table_name completed successfully." | tee -a "$log_file"
  else
    echo "$table_name failed." | tee -a "$log_file"
  fi
done

echo "All jobs completed. Logs are available in $log_dir."
