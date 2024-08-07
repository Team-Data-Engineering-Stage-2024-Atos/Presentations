import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import subprocess
from tqdm import tqdm
import logging

logger = logging.getLogger(__name__)

class WebScraper:
    def __init__(self, urls, download_path, hdfs_path):
        self.urls = urls
        self.download_path = download_path
        self.hdfs_path = hdfs_path

    def scrape(self):
        if not os.path.exists(self.download_path):
            os.makedirs(self.download_path)

        for source, url in tqdm(self.urls.items(), desc="Scraping web data"):
            try:
                source_path = os.path.join(self.download_path, source)
                if not os.path.exists(source_path):
                    os.makedirs(source_path)

                response = requests.get(url)
                soup = BeautifulSoup(response.content, 'html.parser')

                # Implement specific scraping logic based on the structure of the web pages.
                # Below is an example for a table with <tr> rows and <td> columns.

                data = []
                table = soup.find('table')
                for row in table.find_all('tr'):
                    cols = row.find_all('td')
                    data.append([col.text.strip() for col in cols])

                df = pd.DataFrame(data)
                file_name = os.path.join(source_path, f"{source}.csv")
                df.to_csv(file_name, index=False)
                logger.info(f"Scraped and saved {file_name}")

                # Upload to HDFS
                self.upload_to_hdfs(source_path)
            except Exception as e:
                logger.error(f"Error scraping {url}: {e}")

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
    urls = {
        "example": "https://example.com/covid19-data"
    }
    scraper = WebScraper(urls, '../datasets/raw/web', '/user/admin/data/raw/web')
    scraper.scrape()
