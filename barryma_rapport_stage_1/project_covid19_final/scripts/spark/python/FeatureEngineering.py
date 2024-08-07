from pyspark.sql import SparkSession
from pyspark.sql.functions import col, log, to_date

# Create a SparkSession
spark = SparkSession.builder.appName("Feature Engineering").getOrCreate()

# Load the dataset
df = spark.read.option("header", "true").csv(sys.argv[1])

# Feature engineering: Adding new features
engineeredDF = df \
  .withColumn("log_cases", log(col("cases") + 1)) \
  .withColumn("cases_squared", col("cases") * col("cases")) \
  .withColumn("date", to_date(col("date"), "MM/dd/yy"))

# Save the dataset with new features
engineeredDF.write.option("header", "true").csv(sys.argv[2])

spark.stop()
