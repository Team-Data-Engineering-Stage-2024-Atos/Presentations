import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BasicStatistics {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Basic Statistics").getOrCreate()
    
    // Load the dataset
    val df = spark.read.option("header", "true").csv(args(0))
    
    // Calculate basic statistics
    df.describe().show()
    
    // Calculate additional statistics
    val statisticsDF = df.select(
      mean("cases").alias("mean_cases"),
      stddev("cases").alias("stddev_cases"),
      min("cases").alias("min_cases"),
      max("cases").alias("max_cases")
    )
    statisticsDF.show()
    
    // Save the statistics
    statisticsDF.write.option("header", "true").csv(args(1))
    
    spark.stop()
  }
}
