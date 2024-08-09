from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import explode

# Initialize Spark session
spark = SparkSession.builder \
    .appName("RecommenderSystem") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# Load data from Hive
df = spark.sql("SELECT * FROM user_interactions")

# Data preprocessing (filter for purchases)
df_filtered = df.filter(df['EventType'] == 'purchase')

# Indexing UserID and ProductID
user_indexer = StringIndexer(inputCol="UserID", outputCol="userIndex")
df_indexed = user_indexer.fit(df_filtered).transform(df_filtered)

item_indexer = StringIndexer(inputCol="ProductID", outputCol="itemIndex")
df_indexed = item_indexer.fit(df_indexed).transform(df_indexed)

# Fit ALS model
als = ALS(userCol="userIndex", itemCol="itemIndex", ratingCol="Amount", nonnegative=True, coldStartStrategy="drop")
model = als.fit(df_indexed)

# Generate top 10 recommendations for all users
user_recs = model.recommendForAllUsers(10)
user_recs.show()

# Flatten the recommendations DataFrame
user_recs_flattened = user_recs.select("userIndex", explode("recommendations").alias("rec"))
user_recs_flattened = user_recs_flattened.select("userIndex", "rec.itemIndex", "rec.rating")

# Save the recommendations to Hive
user_recs_flattened.write.mode("overwrite").saveAsTable("user_recommendations")
