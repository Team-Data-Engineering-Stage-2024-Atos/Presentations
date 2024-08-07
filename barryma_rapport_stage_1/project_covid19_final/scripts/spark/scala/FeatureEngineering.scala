import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FeatureEngineering {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Feature Engineering").getOrCreate()
    
    // Load the dataset
    val df = spark.read.option("header", "true").csv(args(0))
    
    // Feature engineering: Adding new features
    val engineeredDF = df
      .withColumn("log_cases", log(col("cases") + 1))
      .withColumn("cases_squared", col("cases") * col("cases"))
      .withColumn("date", to_date(col("date"), "MM/dd/yy"))
    
    // Save the dataset with new features
    engineeredDF.write.option("header", "true").csv(args(1))
    
    spark.stop()
  }
}
