package com.datafusion.pipelines

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object SalesAnalysisPipeline {
  def run(spark: SparkSession, salesData: DataFrame): DataFrame = {
    salesData.groupBy("StockCode")
      .agg(
        sum("Quantity").alias("TotalQuantity"),
        sum(col("Quantity") * col("Price")).alias("TotalRevenue")
      )
      .orderBy(desc("TotalRevenue"))
  }
}
