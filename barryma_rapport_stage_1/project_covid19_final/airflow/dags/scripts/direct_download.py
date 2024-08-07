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
