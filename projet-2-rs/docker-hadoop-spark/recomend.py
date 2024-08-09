from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("CollaborativeFiltering") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Load the data from HDFS
data_path = "hdfs://namenode:9000/data/interactions/real/interactions.csv"
data = spark.read.csv(data_path, header=True, inferSchema=True)

# Show the schema to verify correct loading
data.printSchema()

# Filter the data to include only relevant EventTypes
data = data.filter(col("EventType").isin("purchase", "add_to_cart", "product_view"))

# Assign numeric ratings based on EventType
data = data.withColumn("rating", when(col("Outcome") == "purchase", 3)
                                   .when(col("EventType") == "add_to_cart", 2)
                                   .when(col("EventType") == "product_view", 1)
                                   .otherwise(0))

# Convert UserID and ProductID to numeric indices manually
user_ids = data.select("UserID").distinct().rdd.zipWithIndex().toDF(["UserID_struct", "userIndex"])
user_ids = user_ids.selectExpr("UserID_struct.UserID as UserID", "userIndex")

product_ids = data.select("ProductID").distinct().rdd.zipWithIndex().toDF(["ProductID_struct", "productIndex"])
product_ids = product_ids.selectExpr("ProductID_struct.ProductID as ProductID", "productIndex")

# Join indices with the original data
data = data.join(user_ids, on="UserID").join(product_ids, on="ProductID")

# Select and rename columns to match ALS requirements
als_data = data.selectExpr("userIndex as user", "productIndex as item", "rating")

# Filter out rows with null ratings or product IDs
als_data = als_data.filter(als_data.rating.isNotNull() & als_data.item.isNotNull())

# Show the transformed data
als_data.show(5)

from pyspark.ml.recommendation import ALS

# Initialize ALS model
als = ALS(maxIter=10, regParam=0.01, userCol="user", itemCol="item", ratingCol="rating", coldStartStrategy="drop")

# Train the model
model = als.fit(als_data)

# Generate top 10 recommendations for each user
user_recommendations = model.recommendForAllUsers(10)

# Convert recommendations to a more readable format
from pyspark.sql.functions import explode
recommendations = user_recommendations.withColumn("recommendation", explode("recommendations")).select("user", "recommendation.*")

# Show the recommendations
recommendations.show(5)

# Save recommendations to a Hive table
recommendations.createOrReplaceTempView("recommendations")

# Create Hive table and insert data
spark.sql("CREATE TABLE IF NOT EXISTS user_recommendations (user INT, item INT, rating FLOAT)")
spark.sql("INSERT INTO user_recommendations SELECT * FROM recommendations")

# Example query to fetch recommendations for a specific user
user_id = 123  # replace with an actual user id
query = "SELECT * FROM user_recommendations WHERE user = {}".format(user_id)
spark.sql(query).show()
