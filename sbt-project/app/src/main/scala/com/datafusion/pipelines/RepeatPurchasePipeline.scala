package com.datafusion.pipelines

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object RepeatPurchasePipeline {
  def run(spark: SparkSession, salesData: DataFrame): DataFrame = {
    salesData.groupBy("Customer ID")
      .agg(
        countDistinct("Invoice").alias("TotalPurchases"),
        sum("Quantity").alias("TotalQuantity")
      )
      .orderBy(desc("TotalPurchases"))
  }
}
