from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, stddev, min, max

# Create a SparkSession
spark = SparkSession.builder.appName("Basic Statistics").getOrCreate()

# Load the dataset
df = spark.read.option("header", "true").csv("hdfs://namenode:9000/user/spark/test.csv")    #("hdfs://cov19-namenode:9000/user/hadoop/datasets/cleaned")

# Calculate basic statistics
df.describe().show()

# Calculate additional statistics
statisticsDF = df.select(
  mean("cases").alias("mean_cases"),
  stddev("cases").alias("stddev_cases"),
  min("cases").alias("min_cases"),
  max("cases").alias("max_cases")
)
statisticsDF.show()

# Save the statistics
#statisticsDF.write.option("header", "true").csv("hdfs://cov19-namenode:8020/user/hive/warehouse/cov19_basic_statistics")

spark.stop()
