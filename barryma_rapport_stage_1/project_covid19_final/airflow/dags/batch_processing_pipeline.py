from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import subprocess

# Paths and configurations
HDFS_URL = 'hdfs://namenode:9000'
HDFS_PATH_DOWN = '/user/admin/data/raw'
HDFS_PATH_PROC = '/user/admin/data/processed'
DOWNLOAD_PATH = './datasets/raw'
PROCESSED_PATH = './datasets/processed'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    'batch_processing_pipeline',
    default_args=default_args,
    description='A batch processing pipeline with Spark and Hive',
    schedule_interval='@daily',
)

def run_direct_download():
    from downloaders.direct_download import DirectDownloader

    urls = {
        "owid": [
            "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/jhu/full_data.csv"
        ],
        "csse": [
            "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv",
            "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv"
        ],
        "google_cloud": [
            "https://storage.googleapis.com/covid19-open-data/v2/main.csv"
        ],
        "who": [
            "https://covid19.who.int/WHO-COVID-19-global-data.csv"
        ]
    }
    downloader = DirectDownloader(urls, DOWNLOAD_PATH, HDFS_PATH_DOWN)
    downloader.download()

def run_data_cleaning():
    from processors.data_cleaning import DataCleaner

    cleaner = DataCleaner(HDFS_URL, HDFS_PATH_DOWN, HDFS_PATH_PROC)
    cleaner.run_all()

def run_hive_queries():
    from analyzers.hive_queries import HiveQueries

    hive_queries = HiveQueries(host='hive-server', hdfs_path_proc=HDFS_PATH_PROC)
    hive_queries.run_all()

def run_visualizations():
    import pandas as pd
    import matplotlib.pyplot as plt

    # File paths (adjust to your local file paths)
    file_path = f'{HDFS_URL}{HDFS_PATH_PROC}/owid_cleaned_data.parquet'

    # Load data into a pandas DataFrame
    df = pd.read_parquet(file_path)

    # Display the first few rows of the DataFrame
    print(df.head())

    # Ensure that 'date', 'total_cases', and 'total_deaths' columns exist
    # If column names are different, adjust them accordingly
    df['date'] = pd.to_datetime(df['date'])  # Ensure 'date' column is in datetime format

    # Plot total cases and deaths over time
    plt.figure(figsize=(12, 6))
    plt.plot(df['date'], df['total_cases'], label='Total Cases', color='blue')
    plt.plot(df['date'], df['total_deaths'], label='Total Deaths', color='red')
    plt.xlabel('Date')
    plt.ylabel('Count')
    plt.title('COVID-19 Total Cases and Deaths Over Time')
    plt.legend()
    plt.show()

download_task = PythonOperator(
    task_id='download_data',
    python_callable=run_direct_download,
    dag=dag,
)

clean_task = PythonOperator(
    task_id='clean_data',
    python_callable=run_data_cleaning,
    dag=dag,
)

hive_task = PythonOperator(
    task_id='hive_queries',
    python_callable=run_hive_queries,
    dag=dag,
)

visualization_task = PythonOperator(
    task_id='generate_visualizations',
    python_callable=run_visualizations,
    dag=dag,
)

download_task >> clean_task >> hive_task >> visualization_task
