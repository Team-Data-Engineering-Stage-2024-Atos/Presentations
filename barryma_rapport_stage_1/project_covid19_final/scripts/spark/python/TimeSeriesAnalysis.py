from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, lag, col
from pyspark.sql.window import Window
import sys

# Create a SparkSession
spark = SparkSession.builder.appName("Time Series Analysis").getOrCreate()

# Load the datasets
df = spark.read.option("header", "true").csv(sys.argv[1])

# Example time series analysis: Calculate moving average and lag features
windowSpec = Window.orderBy("date").rowsBetween(-3, 3)
timeSeriesDF = df \
  .withColumn("moving_avg", avg("cases").over(windowSpec)) \
  .withColumn("cases_lag_1", lag("cases", 1).over(windowSpec)) \
  .withColumn("cases_diff", col("cases") - lag("cases", 1).over(windowSpec))

# Save the time series dataset
timeSeriesDF.write.option("header", "true").csv(sys.argv[2])

spark.stop()
