package com.datafusion.pipelines

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class RepeatPurchasePipelineTest extends FunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var salesData: DataFrame = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder
      .appName("RepeatPurchasePipelineTest")
      .master("local[*]")
      .getOrCreate()

    salesData = spark.read.option("header", "true").csv("src/test/resources/datasets/mock_online_retail_II.csv")
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("run should correctly aggregate repeat purchases data") {
    val result = RepeatPurchasePipeline.run(spark, salesData)
    assert(result.columns.contains("TotalPurchases"))
    assert(result.columns.contains("TotalQuantity"))
  }
}
