import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataAggregation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Data Aggregation").getOrCreate()
    
    // Load the dataset
    val df = spark.read.option("header", "true").csv(args(0))
    
    // Example aggregation: Group by a column and calculate multiple aggregates
    val aggregatedDF = df.groupBy("country")
      .agg(
        sum("cases").as("total_cases"),
        avg("cases").as("avg_cases"),
        max("cases").as("max_cases"),
        min("cases").as("min_cases")
      )
    
    // Save the aggregated dataset
    aggregatedDF.write.option("header", "true").csv(args(1))
    
    spark.stop()
  }
}
