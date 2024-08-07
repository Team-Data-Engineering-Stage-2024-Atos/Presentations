from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import expr


# Read Data from HDFS
spark = SparkSession.builder \
    .appName("CovidAdvancedProcessing") \
    .master("spark://spark-master:7077") \
    .enableHiveSupport() \
    .getOrCreate()


confirmed_df = spark.read.csv("hdfs://namenode:9000/user/hadoop/covid_data/output_confirmed_cases_sample/part-r-00000", header=False, inferSchema=True)
deaths_df = spark.read.csv("hdfs://namenode:9000/user/hadoop/covid_data/output_deaths_sample/part-r-00000", header=False, inferSchema=True)


# Define Schema and Rename Columns
confirmed_df = confirmed_df.withColumnRenamed("_c0", "state_date") \
                           .withColumnRenamed("_c1", "confirmed_cases") \
                           .withColumnRenamed("_c2", "deaths") \
                           .withColumnRenamed("_c3", "population")

deaths_df = deaths_df.withColumnRenamed("_c0", "state_date") \
                     .withColumnRenamed("_c1", "confirmed_cases") \
                     .withColumnRenamed("_c2", "deaths") \
                     .withColumnRenamed("_c3", "population")



# Perform Advanced Transformations
# Split state_date into separate columns
confirmed_df = confirmed_df.withColumn("state", expr("split(state_date, ',')[0]")) \
                           .withColumn("date", expr("split(state_date, ',')[1]"))

deaths_df = deaths_df.withColumn("state", expr("split(state_date, ',')[0]")) \
                     .withColumn("date", expr("split(state_date, ',')[1]"))

# Calculate death rate and confirmed cases per capita
confirmed_df = confirmed_df.withColumn("death_rate", col("deaths") / col("confirmed_cases") * 100) \
                           .withColumn("cases_per_capita", col("confirmed_cases") / col("population"))

deaths_df = deaths_df.withColumn("death_rate", col("deaths") / col("confirmed_cases") * 100) \
                     .withColumn("cases_per_capita", col("confirmed_cases") / col("population"))


# Write Data to Hive
confirmed_df.write.mode("overwrite").saveAsTable("covid_confirmed_cases_sample")
deaths_df.write.mode("overwrite").saveAsTable("covid_deaths_sample")

