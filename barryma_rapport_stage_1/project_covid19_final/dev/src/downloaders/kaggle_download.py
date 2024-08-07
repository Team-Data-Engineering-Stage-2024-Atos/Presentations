import os
from kaggle.api.kaggle_api_extended import KaggleApi
import subprocess
from tqdm import tqdm
import logging

logger = logging.getLogger(__name__)

class KaggleDownloader:
    def __init__(self, datasets, download_path, hdfs_path):
        self.datasets = datasets
        self.download_path = download_path
        self.hdfs_path = hdfs_path
        self.api = KaggleApi()
        self.api.authenticate()

    def download(self):
        if not os.path.exists(self.download_path):
            os.makedirs(self.download_path)

        for dataset in tqdm(self.datasets, desc="Downloading Kaggle datasets"):
            try:
                dataset_path = os.path.join(self.download_path, dataset.split('/')[-1])
                if not os.path.exists(dataset_path):
                    os.makedirs(dataset_path)

                self.api.dataset_download_files(dataset, path=dataset_path, unzip=True)
                logger.info(f"Downloaded {dataset} to {dataset_path}")

                # Upload to HDFS
                self.upload_to_hdfs(dataset_path)
            except Exception as e:
                logger.error(f"Error downloading {dataset}: {e}")

    def upload_to_hdfs(self, local_dir):
        try:
            # Create HDFS directories if they don't exist
            subprocess.run(["hadoop", "fs", "-mkdir", "-p", self.hdfs_path], check=True)

            # Upload files to HDFS
            for root, dirs, files in os.walk(local_dir):
                for file in files:
                    local_file_path = os.path.join(root, file)
                    hdfs_file_path = os.path.join(self.hdfs_path, os.path.relpath(local_file_path, local_dir))
                    subprocess.run(["hadoop", "fs", "-put", local_file_path, hdfs_file_path], check=True)
                    logger.info(f"Uploaded {local_file_path} to HDFS at {hdfs_file_path}")
        except subprocess.CalledProcessError as e:
            logger.error(f"Error uploading files to HDFS from {local_dir}: {e}")

if __name__ == "__main__":
    datasets = [
        "sudalairajkumar/covid19-in-india",
        "nimishnag/covid19-latest"
    ]
    downloader = KaggleDownloader(datasets, '../datasets/raw/kaggle', '/user/admin/data/raw/kaggle')
    downloader.download()
