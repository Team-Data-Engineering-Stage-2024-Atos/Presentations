package com.datafusion.pipelines

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger

object CLTVPipeline {
  def run(spark: SparkSession, salesData: DataFrame, customerData: DataFrame): DataFrame = {
    val logger = Logger.getLogger(getClass.getName)
    logger.info("Running CLTV Pipeline...")

    val salesPerCustomer = salesData.groupBy("Customer ID")
      .agg(
        sum("Quantity").alias("TotalQuantity"),
        sum("Price").alias("TotalRevenue")
      )

    val cltv = salesPerCustomer.join(customerData, salesPerCustomer("Customer ID") === customerData("CUST_ID"))
      .withColumn("CLTV", col("TotalRevenue") * col("TENURE"))
      .select("Customer ID", "CLTV")

    logger.info(s"CLTV Pipeline completed. Resulting DataFrame has ${cltv.count()} rows.")
    
    cltv
  }
}
