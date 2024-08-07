from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, expr
import logging
import re

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataCleaner:
    def __init__(self, hdfs_url, raw_path, processed_path):
        self.hdfs_url = hdfs_url
        self.raw_path = raw_path
        self.processed_path = processed_path

    def is_date_column(self, column_name):
        pattern = re.compile(r'\d{1,2}/\d{1,2}/\d{2}')
        return bool(pattern.fullmatch(column_name))

    def reshape_dataframe(self, df, fixed_columns, date_columns):
        stack_expr = "stack({0}, {1}) as (Date, Deaths)".format(
            len(date_columns),
            ', '.join([f"'{col}' as `{col}`" for col in date_columns])
        )
        df_reshaped = df.selectExpr(*fixed_columns, stack_expr)
        return df_reshaped

    def clean_owid_data(self, spark):
        try:
            df = spark.read.csv(f"{self.hdfs_url}{self.raw_path}/owid/full_data.csv", header=True, inferSchema=True)
            df_cleaned = df.dropna().withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
            df_cleaned.write.mode("overwrite").parquet(f"{self.hdfs_url}{self.processed_path}/owid_cleaned_data.parquet")
            logger.info("OWID data cleaned and saved.")
        except Exception as e:
            logger.error(f"Error cleaning OWID data: {e}")

    def clean_google_cloud_data(self, spark):
        try:
            df = spark.read.csv(f"{self.hdfs_url}{self.raw_path}/google_cloud/main.csv", header=True, inferSchema=True)
            df_cleaned = df.dropna().withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
            df_cleaned.write.mode("overwrite").parquet(f"{self.hdfs_url}{self.processed_path}/google_cloud_cleaned_data.parquet")
            logger.info("Google Cloud data cleaned and saved.")
        except Exception as e:
            logger.error(f"Error cleaning Google Cloud data: {e}")

    def clean_csse_data(self, spark):
        try:
            confirmed_df = spark.read.csv(f"{self.hdfs_url}{self.raw_path}/csse/time_series_covid19_confirmed_global.csv", header=True, inferSchema=True)
            deaths_df = spark.read.csv(f"{self.hdfs_url}{self.raw_path}/csse/time_series_covid19_deaths_global.csv", header=True, inferSchema=True)
            
            confirmed_df = confirmed_df.dropna()
            deaths_df = deaths_df.dropna()

            fixed_columns = [col for col in confirmed_df.columns if not self.is_date_column(col)]
            confirmed_date_columns = [col for col in confirmed_df.columns if self.is_date_column(col)]
            deaths_date_columns = [col for col in deaths_df.columns if self.is_date_column(col)]

            confirmed_df_reshaped = self.reshape_dataframe(confirmed_df, fixed_columns, confirmed_date_columns)
            deaths_df_reshaped = self.reshape_dataframe(deaths_df, fixed_columns, deaths_date_columns)

            confirmed_df_reshaped.write.mode("overwrite").parquet(f"{self.hdfs_url}{self.processed_path}/csse_confirmed_cleaned_data.parquet")
            deaths_df_reshaped.write.mode("overwrite").parquet(f"{self.hdfs_url}{self.processed_path}/csse_deaths_cleaned_data.parquet")

            logger.info("CSSE data cleaned and saved.")
        except Exception as e:
            logger.error(f"Error cleaning CSSE data: {e}")

    def clean_who_data(self, spark):
        try:
            df = spark.read.csv(f"{self.hdfs_url}{self.raw_path}/who/WHO-COVID-19-global-data.csv", header=True, inferSchema=True)
            df_cleaned = df.dropna().withColumn("Date_reported", to_date(col("Date_reported"), "yyyy-MM-dd"))
            df_cleaned.write.mode("overwrite").parquet(f"{self.hdfs_url}{self.processed_path}/who_cleaned_data.parquet")
            logger.info("WHO data cleaned and saved.")
        except Exception as e:
            logger.error(f"Error cleaning WHO data: {e}")

    def run_all(self):
        spark = SparkSession.builder \
            .appName("CovidDataCleaning") \
            .config("spark.hadoop.fs.defaultFS", self.hdfs_url) \
            .getOrCreate()

        self.clean_owid_data(spark)
        self.clean_google_cloud_data(spark)
        self.clean_csse_data(spark)
        self.clean_who_data(spark)

        spark.stop()

if __name__ == "__main__":
    hdfs_url = 'hdfs://namenode:9000'
    raw_path = '/user/admin/data/raw'
    processed_path = '/user/admin/data/processed'
    cleaner = DataCleaner(hdfs_url, raw_path, processed_path)
    cleaner.run_all()
