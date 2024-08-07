from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys

# Create a SparkSession
spark = SparkSession.builder.appName("Data Cleaning").getOrCreate()

# Load the dataset
df = spark.read.option("header", "true").csv(sys.argv[1])

# Handle missing values
cleanedDF = df.na.fill("0").na.replace("*", {"": "0"})

# Additional data cleaning steps
furtherCleanedDF = cleanedDF \
  .withColumn("cases", col("cases").cast("int")) \
  .filter("cases >= 0")

# Save the cleaned dataset
furtherCleanedDF.write.option("header", "true").csv(sys.argv[2])

spark.stop()
