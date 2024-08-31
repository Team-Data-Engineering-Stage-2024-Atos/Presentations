#!/bin/bash

config_file="src/main/resources/application.conf"

batch=("dim_customers" "dim_products" "dim_dates" "fact_sales" "sales_mart" "customer_loyalty_mart" "product_performance_mart")


log_dir="spark_logs_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$log_dir"

run_spark_job() {
  local table_name=$1
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
    exit 1 
  fi
}


for table_name in "${batch[@]}"; do
  run_spark_job "$table_name"
done

echo "All jobs completed. Logs are available in $log_dir."
