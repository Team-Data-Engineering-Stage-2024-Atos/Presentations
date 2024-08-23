package com.datafusion.pipelines

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class ProductPerformancePipelineTest extends FunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var salesData: DataFrame = _
  private var productData: DataFrame = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder
      .appName("ProductPerformancePipelineTest")
      .master("local[*]")
      .getOrCreate()

    salesData = spark.read.option("header", "true").csv("src/test/resources/datasets/mock_online_retail_II.csv")
    productData = spark.read.option("header", "true").csv("src/test/resources/datasets/mock_amazon-product-listing-data.csv")
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("run should correctly join and aggregate data") {
    val result = ProductPerformancePipeline.run(spark, salesData, productData)
    assert(result.columns.contains("TotalSales"))
    assert(result.columns.contains("AmazonRating"))
  }
}
