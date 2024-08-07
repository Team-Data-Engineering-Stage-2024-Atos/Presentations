import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("DistributedFileTest")
  .getOrCreate()

val df = spark.read.option("header", "true").csv("hdfs://namenode:9000/user/hadoop/cov19_data/cleaned/agg_monthly_us_deaths_sample.csv")
df.show(5)
