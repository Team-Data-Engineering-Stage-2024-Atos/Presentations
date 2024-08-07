import os
import json
import logging
import webbrowser
import subprocess
import re
from subprocess import run
from downloaders.direct_download import DirectDownloader
from downloaders.kaggle_download import KaggleDownloader
from downloaders.web_scraping import WebScraper
from processors.data_cleaning import DataCleaner
from analyzers.hive_queries import HiveQueries

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define paths
HDFS_URL='hdfs://namenode:9000'
HDFS_PATH_DOWN = '/user/admin/data/raw'
HDFS_PATH_PROC = '/user/admin/data/processed'
DOWNLOAD_PATH = './datasets/raw'
PROCESSED_PATH = './datasets/processed'

def main_menu():
    print("\nCOVID-19 Data Pipeline Menu")
    print("1. Direct Download")
    print("2. Kaggle Download")
    print("3. Web Scraping")
    print("4. Data Processing")
    print("5. Data Analysis")
    print("6. Jupyter")
    print("7. Exit")

    choice = input("Enter your choice: ")
    return choice

def direct_download_menu():
    print("\nDirect Download Menu")
    print("Enter the URLs in JSON format, e.g.,")
    print('{"owid": ["https://url1", "https://url2"], "google_cloud": ["https://url3"]}')
    urls = input("Enter the URLs: ")
    urls = json.loads(urls)
    logger.info("Starting Direct Download...")
    downloader = DirectDownloader(urls, DOWNLOAD_PATH, HDFS_PATH_DOWN)
    downloader.download()
    logger.info("Direct Download completed.")

def kaggle_download_menu():
    print("\nKaggle Download Menu")
    print('Enter the Kaggle datasets in a list format, e.g., ["username/dataset1", "username/dataset2"]')
    datasets = input("Enter the Kaggle datasets: ")
    datasets = json.loads(datasets)
    logger.info("Starting Kaggle Download...")
    downloader = KaggleDownloader(datasets, DOWNLOAD_PATH)
    downloader.download()
    logger.info("Kaggle Download completed.")

def web_scraping_menu():
    print("\nWeb Scraping Menu")
    print('Enter the URLs in JSON format, e.g., {"example": "https://example.com/data"}')
    urls = input("Enter the URLs: ")
    urls = json.loads(urls)
    logger.info("Starting Web Scraping...")
    scraper = WebScraper(urls, DOWNLOAD_PATH)
    scraper.scrape()
    logger.info("Web Scraping completed.")

def data_processing_menu():
    print("\nData Processing Menu")
    logger.info("Starting Data Processing...")
    try:
        cleaner = DataCleaner(HDFS_URL, HDFS_PATH_DOWN, HDFS_PATH_PROC)
        cleaner.run_all()
        logger.info("Data Processing completed.")
    except Exception as e:
        logger.error(f"Error during data processing: {e}")


def data_analysis_menu():
    print("\nData Analysis Menu")
    logger.info("Starting Data Analysis...")
    try:
        # Create HiveQueries instance with necessary arguments
        hive_queries = HiveQueries(host='hive-server', hdfs_path_proc=HDFS_PATH_PROC)
        hive_queries.run_all()
        logger.info("Data Analysis completed.")
    except Exception as e:
        logger.error(f"Error during data analysis: {e}")


def jupyter_menu():
    # bash_command = "docker exec spark-notebook jupyter server list"
    # process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    # output = process.communicate()
    # token = re.search(r'token:.*', output.decode()).group(0)
    # print("token = ", token)
    
    print('url = http://localhost:8888')
    #webbrowser.get('firefox').open(url)


def test_hive_queries():
    host = 'hive-server'
    hdfs_path_proc = '/user/admin/data/processed/test'

    hive_queries = HiveQueries(host=host, hdfs_path_proc=hdfs_path_proc)

    custom_schema = """
    id INT,
    name STRING,
    value DOUBLE
    """
    custom_table_name = 'custom_test_table'
    custom_hdfs_path = f'{hdfs_path_proc}/custom_test_data'

    try:
        hive_queries.create_table(custom_table_name, custom_schema, custom_hdfs_path)
        print(f"Custom table '{custom_table_name}' created successfully.")
    except Exception as e:
        print(f"An error occurred while creating custom table: {e}")

def main():
    while True:
        choice = main_menu()
        if choice == '1':
            direct_download_menu()
        elif choice == '2':
            kaggle_download_menu()
        elif choice == '3':
            web_scraping_menu()
        elif choice == '4':
            data_processing_menu()
        elif choice == '5':
            data_analysis_menu()
            #test_hive_queries()
        elif choice == '6':
            jupyter_menu()
        elif choice == '7':
            print("Exiting...")
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()
