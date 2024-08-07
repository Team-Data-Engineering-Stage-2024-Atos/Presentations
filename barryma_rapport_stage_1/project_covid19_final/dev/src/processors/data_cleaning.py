from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import IntegerType
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

    def transform_date_columns(self, df):
        for column in df.columns:
            if self.is_date_column(column):
                df = df.withColumnRenamed(column, f"date_{column.replace('/', '_')}")
        return df

    def clean_and_transform(self, df, date_column=None, int_columns=None):
        df_cleaned = df.dropna()
        if date_column:
            df_cleaned = df_cleaned.withColumn(date_column, to_date(col(date_column), "yyyy-MM-dd"))
        if int_columns:
            for col_name in int_columns:
                df_cleaned = df_cleaned.withColumn(col_name, col(col_name).cast(IntegerType()))
        return df_cleaned

    def reshape_dataframe(self, df, fixed_columns, date_columns):
        stack_expr = "stack({0}, {1}) as (Date, Value)".format(
            len(date_columns),
            ', '.join([f"'{col}' as `{col}`" for col in date_columns])
        )
        df_reshaped = df.selectExpr(*fixed_columns, stack_expr)
        df_reshaped = self.transform_date_columns(df_reshaped)  # Rename columns in the reshaped dataframe
        return df_reshaped

    def process_data(self, spark, dataset_name, date_column=None, reshape=False, int_columns=None):
        try:
            logger.info(f"Processing dataset: {dataset_name}")
            df = spark.read.csv(f"{self.hdfs_url}{self.raw_path}/{dataset_name}", header=True, inferSchema=True)
            df_cleaned = self.clean_and_transform(df, date_column, int_columns)
            
            if reshape:
                fixed_columns = [col for col in df_cleaned.columns if not self.is_date_column(col)]
                date_columns = [col for col in df_cleaned.columns if self.is_date_column(col)]
                df_cleaned = self.reshape_dataframe(df_cleaned, fixed_columns, date_columns)

            # Count and log the number of date columns
            date_columns_count = sum(1 for col in df_cleaned.columns if 'date_' in col)
            logger.info(f"{dataset_name} has {date_columns_count} date columns after processing.")

            output_path = f"{self.hdfs_url}{self.processed_path}/{dataset_name.replace('.csv', '_cleaned.parquet')}"
            df_cleaned.write.mode("overwrite").parquet(output_path)
            logger.info(f"{dataset_name} cleaned and saved to {output_path}")
        except Exception as e:
            logger.error(f"Error cleaning {dataset_name}: {e}")

    def run_all(self):
        spark = SparkSession.builder \
            .appName("CovidDataCleaning") \
            .config("spark.hadoop.fs.defaultFS", self.hdfs_url) \
            .getOrCreate()

        self.process_data(spark, 'owid/full_data.csv', date_column='date', int_columns=['new_cases', 'new_deaths', 'total_cases', 'total_deaths'])
        #self.process_data(spark, 'google_cloud/main.csv', date_column='date', int_columns=['new_cases', 'new_deaths', 'total_cases', 'total_deaths'])
        #self.process_data(spark, 'csse/time_series_covid19_confirmed_global.csv', reshape=True, int_columns=['cases'])
        #self.process_data(spark, 'csse/time_series_covid19_deaths_global.csv', reshape=True, int_columns=['deaths'])
        self.process_data(spark, 'who/WHO-COVID-19-global-data.csv', date_column='Date_reported', int_columns=['new_cases', 'new_deaths', 'cumulative_cases', 'cumulative_deaths'])

        spark.stop()

if __name__ == "__main__":
    hdfs_url = 'hdfs://namenode:9000'
    raw_path = '/user/admin/data/raw'
    processed_path = '/user/admin/data/processed'
    cleaner = DataCleaner(hdfs_url, raw_path, processed_path)
    cleaner.run_all()
