package com.datafusion.pipelines

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object CLTVPipeline {
  def run(spark: SparkSession, salesData: DataFrame, customerData: DataFrame): DataFrame = {
    val salesPerCustomer = salesData.groupBy("Customer ID")
      .agg(
        sum("Quantity").alias("TotalQuantity"),
        sum("Price").alias("TotalRevenue")
      )
    
    val cltv = salesPerCustomer.join(customerData, salesPerCustomer("Customer ID") === customerData("CUST_ID"))
      .withColumn("CLTV", col("TotalRevenue") * col("TENURE")) // Example calculation
      .select("Customer ID", "CLTV")

    cltv
  }
}
