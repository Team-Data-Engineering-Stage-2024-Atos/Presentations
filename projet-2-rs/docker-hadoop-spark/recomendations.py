# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import col

# Créer une session Spark
spark = SparkSession.builder.appName("EcommerceCollaborativeFiltering").getOrCreate()

# Charger les données
data = spark.read.csv("hdfs://namenode:9000/data/interactions/real/interactions.csv", header=True, inferSchema=True, encoding="utf-8")

# Préparer les données
# On suppose que 'Outcome' est la colonne représentant l'interaction (par exemple, achat, clic, etc.)
interactions = data.select(col("UserID").alias("user"), col("ProductID").alias("item"), col("Outcome").alias("interaction"))

# Diviser les données en ensembles d'entraînement et de test
(training, test) = interactions.randomSplit([0.8, 0.2])

# Construire le modèle ALS
als = ALS(maxIter=10, regParam=0.1, userCol="user", itemCol="item", ratingCol="interaction", coldStartStrategy="drop")
model = als.fit(training)

# Faire des prédictions
predictions = model.transform(test)

# Évaluer le modèle
evaluator = RegressionEvaluator(metricName="rmse", labelCol="interaction", predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = {}".format(rmse))

# Générer des recommandations pour tous les utilisateurs
userRecs = model.recommendForAllUsers(10)
userRecs.show()

# Générer des recommandations pour tous les items
itemRecs = model.recommendForAllItems(10)
itemRecs.show()

# Arrêter la session Spark
spark.stop()
