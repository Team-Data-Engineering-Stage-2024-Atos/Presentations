from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, max, min

# Create a SparkSession
spark = SparkSession.builder.appName("Data Aggregation").getOrCreate()

# Load the dataset
df = spark.read.option("header", "true").csv(sys.argv[1])

# Example aggregation: Group by a column and calculate multiple aggregates
aggregatedDF = df.groupBy("country") \
  .agg(
    sum("cases").alias("total_cases"),
    avg("cases").alias("avg_cases"),
    max("cases").alias("max_cases"),
    min("cases").alias("min_cases")
  )

# Save the aggregated dataset
aggregatedDF.write.option("header", "true").csv(sys.argv[2])

spark.stop()
