package com.datafusion.pipelines

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.Logger

object AnomalyDetectionPipeline {
  def run(spark: SparkSession, customerData: DataFrame): DataFrame = {
    val logger = Logger.getLogger(getClass.getName)
    logger.info("Running Anomaly Detection Pipeline...")

    val filteredData = customerData.filter("CASH_ADVANCE > 0 AND BALANCE > CREDIT_LIMIT")
      .select("CUST_ID", "BALANCE", "CREDIT_LIMIT", "CASH_ADVANCE")

    logger.info(s"Anomaly Detection Pipeline completed. Resulting DataFrame has ${filteredData.count()} rows.")
    
    filteredData
  }
}
