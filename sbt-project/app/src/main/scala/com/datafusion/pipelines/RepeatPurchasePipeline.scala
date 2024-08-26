package com.datafusion.pipelines

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger

object RepeatPurchasePipeline {
  def run(spark: SparkSession, salesData: DataFrame): DataFrame = {
    val logger = Logger.getLogger(getClass.getName)
    logger.info("Running Repeat Purchase Pipeline...")

    val result = salesData.groupBy("Customer ID")
      .agg(
        countDistinct("Invoice").alias("TotalPurchases"),
        sum("Quantity").alias("TotalQuantity")
      )
      .orderBy(desc("TotalPurchases"))

    logger.info(s"Repeat Purchase Pipeline completed. Resulting DataFrame has ${result.count()} rows.")
    
    result
  }
}
