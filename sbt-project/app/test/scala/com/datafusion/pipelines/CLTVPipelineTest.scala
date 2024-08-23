package com.datafusion.pipelines

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class CLTVPipelineTest extends FunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var salesData: DataFrame = _
  private var customerData: DataFrame = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder
      .appName("CLTVPipelineTest")
      .master("local[*]")
      .getOrCreate()

    salesData = spark.read.option("header", "true").csv("src/test/resources/datasets/mock_online_retail_II.csv")
    customerData = spark.read.option("header", "true").csv("src/test/resources/datasets/mock_ccdata.csv")
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("run should correctly calculate CLTV") {
    val result = CLTVPipeline.run(spark, salesData, customerData)
    assert(result.columns.contains("CLTV"))
  }
}
