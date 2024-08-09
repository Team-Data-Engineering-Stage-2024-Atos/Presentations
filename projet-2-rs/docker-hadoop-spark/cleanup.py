from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("RecommenderSystem") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# Load data from Hive
df = spark.sql("SELECT * FROM user_interactions")

# Data preprocessing (filter for specific event type and convert Amount to rating)
df_filtered = df.filter(df['EventType'] == 'purchase')

# Indexing UserID and ProductID
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS

user_indexer = StringIndexer(inputCol="UserID", outputCol="userIndex")
df_indexed = user_indexer.fit(df_filtered).transform(df_filtered)

item_indexer = StringIndexer(inputCol="ProductID", outputCol="itemIndex")
df_indexed = item_indexer.fit(df_indexed).transform(df_indexed)

# Fit ALS model
als = ALS(userCol="userIndex", itemCol="itemIndex", ratingCol="Amount", nonnegative=True, coldStartStrategy="drop")
model = als.fit(df_indexed)
