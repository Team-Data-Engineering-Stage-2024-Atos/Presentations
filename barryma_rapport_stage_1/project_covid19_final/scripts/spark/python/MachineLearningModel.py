from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# Create a SparkSession
spark = SparkSession.builder.appName("Machine Learning Model").getOrCreate()

# Load the dataset
df = spark.read.option("header", "true").csv(sys.argv[1])

# Feature engineering: Assembling features into a feature vector
assembler = VectorAssembler(inputCols=["feature1", "feature2", "feature3"], outputCol="features")
featureDF = assembler.transform(df)

# Split the data into training and test sets
trainingData, testData = featureDF.randomSplit([0.7, 0.3])

# Train a linear regression model
lr = LinearRegression(labelCol="label", featuresCol="features")
lrModel = lr.fit(trainingData)

# Make predictions on the test set
predictions = lrModel.transform(testData)

# Evaluate the model
evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

# Save the model
lrModel.write().overwrite().save(sys.argv[2])

spark.stop()
