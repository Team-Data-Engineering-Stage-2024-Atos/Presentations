import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataCleaning {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Data Cleaning").getOrCreate()
    
    // Load the dataset
    val df = spark.read.option("header", "true").csv(args(0))
    
    // Handle missing values
    val cleanedDF = df.na.fill("0").na.replace("*", Map("" -> "0"))
    
    // Additional data cleaning steps
    val furtherCleanedDF = cleanedDF
      .withColumn("cases", col("cases").cast("int"))
      .filter("cases >= 0")
    
    // Save the cleaned dataset
    furtherCleanedDF.write.option("header", "true").csv(args(1))
    
    spark.stop()
  }
}
