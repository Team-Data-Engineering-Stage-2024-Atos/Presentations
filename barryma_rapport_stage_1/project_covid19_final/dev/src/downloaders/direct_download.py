import os
import requests
import subprocess
from tqdm import tqdm
import logging

logger = logging.getLogger(__name__)

class DirectDownloader:
    def __init__(self, urls, download_path, hdfs_path):
        self.urls = urls
        self.download_path = download_path
        self.hdfs_path = hdfs_path

    def download(self):
        if not os.path.exists(self.download_path):
            os.makedirs(self.download_path)

        for source, links in self.urls.items():
            source_path = os.path.join(self.download_path, source)
            if not os.path.exists(source_path):
                os.makedirs(source_path)

            for link in tqdm(links, desc=f"Downloading {source}"):
                try:
                    file_name = os.path.join(source_path, link.split('/')[-1])
                    response = requests.get(link)
                    with open(file_name, 'wb') as file:
                        file.write(response.content)
                    logger.info(f"Downloaded {file_name}")

                    # Upload to HDFS
                    self.upload_to_hdfs(file_name, os.path.join(self.hdfs_path, source))
                except Exception as e:
                    logger.error(f"Error downloading {link}: {e}")

    def upload_to_hdfs(self, local_file, hdfs_dir):
        try:
            # Create HDFS directories if they don't exist
            subprocess.run(["hadoop", "fs", "-mkdir", "-p", f'hdfs://namenode:9000{hdfs_dir}'], check=True)

            # Upload file to HDFS
            subprocess.run(["hadoop", "fs", "-put", local_file, f'hdfs://namenode:9000{hdfs_dir}'], check=True)
            logger.info(f"Uploaded {local_file} to HDFS at {hdfs_dir}")
        except subprocess.CalledProcessError as e:
            logger.error(f"Error uploading {local_file} to HDFS: {e}")

if __name__ == "__main__":
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
    downloader = DirectDownloader(urls, '../datasets/raw', '/user/admin/data/raw')
    downloader.download()

# "owid": ["https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/jhu/full_data.csv"],"who": ["https://covid19.who.int/WHO-COVID-19-global-data.csv"]}
