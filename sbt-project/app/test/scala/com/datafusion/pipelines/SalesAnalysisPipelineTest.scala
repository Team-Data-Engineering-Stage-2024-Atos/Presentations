package com.datafusion.pipelines

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class SalesAnalysisPipelineTest extends FunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var salesData: DataFrame = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder
      .appName("SalesAnalysisPipelineTest")
      .master("local[*]")
      .getOrCreate()

    salesData = spark.read.option("header", "true").csv("src/test/resources/datasets/mock_online_retail_II.csv")
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("run should correctly aggregate sales data") {
    val result = SalesAnalysisPipeline.run(spark, salesData)
    assert(result.columns.contains("TotalRevenue"))
    assert(result.columns.contains("TotalQuantity"))
  }
}
