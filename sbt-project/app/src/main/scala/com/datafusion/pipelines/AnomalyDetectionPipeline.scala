package com.datafusion.pipelines

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object AnomalyDetectionPipeline {
  def run(spark: SparkSession, customerData: DataFrame): DataFrame = {
    customerData.filter("CASH_ADVANCE > 0 AND BALANCE > CREDIT_LIMIT")
      .select("CUST_ID", "BALANCE", "CREDIT_LIMIT", "CASH_ADVANCE")
  }
}
