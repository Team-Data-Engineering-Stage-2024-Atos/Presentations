#!/bin/bash

usage() {
  echo "Usage: $0 -c <config_file> [-m <deploy_mode>] [-s <spark_options>] [-p] [-P <max_parallel_jobs>] [-r <retry_count>] [-h]"
  echo "  -c <config_file>        Path to the configuration file"
  echo "  -m <deploy_mode>        Deploy mode for Spark (default: client)"
  echo "  -s <spark_options>      Additional Spark options (e.g., --executor-memory 2G)"
  echo "  -p                      Enable parallel processing for each batch"
  echo "  -P <max_parallel_jobs>  Maximum number of parallel jobs (default: 2)"
  echo "  -r <retry_count>        Number of retries for failed jobs (default: 3)"
  echo "  -h                      Display this help message"
  exit 1
}

batches=("dim_customers" "dim_products" "dim_dates" "fact_sales" "sales_mart" "customer_loyalty_mart" "product_performance_mart")

deploy_mode="client"
spark_opts=""
parallel=false
max_parallel_jobs=2
retry_count=3

successful_jobs=()
failed_jobs=()
spark_pid=""

cleanup() {
  echo "Script interrupted. Cleaning up..."
  if [ -n "$spark_pid" ]; then
    echo "Killing spark-submit job with PID $spark_pid"
    kill -9 "$spark_pid"
  fi
  exit 2
}

trap cleanup INT

while getopts "c:m:s:pP:r:h" opt; do
  case $opt in
    c) config_file="$OPTARG" ;;
    m) deploy_mode="$OPTARG" ;;
    s) spark_opts="$OPTARG" ;;
    p) parallel=true ;;
    P) max_parallel_jobs="$OPTARG" ;;
    r) retry_count="$OPTARG" ;;
    h) usage ;;
    *) usage ;;
  esac
done

if [ -z "$config_file" ]; then
  echo "Error: Missing required argument -c (configuration file)."
  usage
fi

if ! command -v spark-submit &> /dev/null; then
  echo "Error: spark-submit could not be found. Ensure Spark is installed and added to your PATH."
  exit 1
fi

if [ ! -f "$config_file" ]; then
  echo "Error: Configuration file '$config_file' does not exist."
  exit 1
fi

log_dir="spark_logs_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$log_dir"

retry_job() {
  local table_name=$1
  local log_file=$2
  local attempts=0

  until [ $attempts -ge $retry_count ]
  do
    spark-submit \
      --class Main \
      --deploy-mode "$deploy_mode" \
      $spark_opts \
      target/scala-2.11/app2-assembly-1.0.jar \
      "$config_file" "$table_name" &> "$log_file" &

    spark_pid=$!

    wait "$spark_pid" 
    exit_code=$?

    if [ $exit_code -eq 0 ]; then
      successful_jobs+=("$table_name")
      spark_pid=""
      return 0
    fi

    attempts=$((attempts+1))
    echo "Retrying job for table $table_name ($attempts/$retry_count)..." | tee -a "$log_file"
  done

  failed_jobs+=("$table_name")
  spark_pid=""
  return 1
}

process_batch() {
  local running_jobs=0

  if [ "$parallel" = true ]; then
    echo "Processing batch in parallel: ${batches[@]}"
    for table_name in "${batches[@]}"; do
      log_file="$log_dir/spark_job_${table_name}.log"
      retry_job "$table_name" "$log_file" &
      running_jobs=$((running_jobs + 1))

      if [ "$running_jobs" -ge "$max_parallel_jobs" ]; then
        wait -n
        running_jobs=$((running_jobs - 1))
      fi
    done
    wait
  else
    echo "Processing batch sequentially: ${batches[@]}"
    for table_name in "${batches[@]}"; do
      log_file="$log_dir/spark_job_${table_name}.log"
      retry_job "$table_name" "$log_file"
    done
  fi
}

generate_summary_report() {
  summary_file="$log_dir/summary_report_$(date +%Y%m%d_%H%M%S).log"
  {
    echo "========================================"
    echo "Job Status Report:"
    echo "========================================"
    total_jobs=0
    for batch_group in "${batches[@]}"; do
      total_jobs=$((total_jobs + ${#batch_group[@]}))
    done
    echo "Total Jobs: $total_jobs"
    echo "Successful Jobs: ${#successful_jobs[@]}"
    echo "Failed Jobs: ${#failed_jobs[@]}"
    echo "----------------------------------------"
    echo "Detailed Job Summary:"
    for job in "${successful_jobs[@]}"; do
      echo "$job: SUCCESS"
    done
    for job in "${failed_jobs[@]}"; do
      echo "$job: FAILURE"
    done
    echo "========================================"
  } > "$summary_file"

  echo "Summary report generated at $summary_file"
}

process_batch
generate_summary_report

if [ ${#failed_jobs[@]} -ne 0 ]; then
  exit 1
else
  read -p "Do you want to clean up the log files? [y/N]: " cleanup_choice
  cleanup_choice=${cleanup_choice:-N}

  if [[ "$cleanup_choice" =~ ^[Yy]$ ]]; then
    rm -rf "$log_dir"
    echo "Log files cleaned up."
  else
    echo "Log files retained in directory: $log_dir"
  fi
  exit 0
fi
