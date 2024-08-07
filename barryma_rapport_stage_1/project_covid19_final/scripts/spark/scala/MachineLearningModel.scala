import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator

object MachineLearningModel {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Machine Learning Model").getOrCreate()
    
    // Load the dataset
    val df = spark.read.option("header", "true").csv(args(0))
    
    // Feature engineering: Assembling features into a feature vector
    val assembler = new VectorAssembler().setInputCols(Array("feature1", "feature2", "feature3")).setOutputCol("features")
    val featureDF = assembler.transform(df)
    
    // Split the data into training and test sets
    val Array(trainingData, testData) = featureDF.randomSplit(Array(0.7, 0.3))
    
    // Train a linear regression model
    val lr = new LinearRegression().setLabelCol("label").setFeaturesCol("features")
    val lrModel = lr.fit(trainingData)
    
    // Make predictions on the test set
    val predictions = lrModel.transform(testData)
    
    // Evaluate the model
    val evaluator = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root Mean Squared Error (RMSE) on test data = $rmse")
    
    // Save the model
    lrModel.write.overwrite().save(args(1))
    
    spark.stop()
  }
}
