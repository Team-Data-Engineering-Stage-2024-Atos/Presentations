package com.datafusion.pipelines

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object ProductPerformancePipeline {
  def run(spark: SparkSession, salesData: DataFrame, productData: DataFrame): DataFrame = {
    val salesDataRenamed = salesData.withColumnRenamed("Price", "SalesPrice")
    val productDataRenamed = productData.withColumnRenamed("Price", "ProductPrice")

    val joinedData = salesDataRenamed.join(productDataRenamed, salesDataRenamed("StockCode") === productDataRenamed("Sku"))

    joinedData.groupBy("StockCode")
      .agg(
        avg("SalesPrice").alias("AverageSalesPrice"),
        sum("Quantity").alias("TotalSales"),
        avg("Average Rating").alias("AmazonRating")
      )
      .orderBy(desc("TotalSales"))
  }
}
