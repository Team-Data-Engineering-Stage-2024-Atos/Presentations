package com.datafusion.pipelines

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class AnomalyDetectionPipelineTest extends FunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var customerData: DataFrame = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder
      .appName("AnomalyDetectionPipelineTest")
      .master("local[*]")
      .getOrCreate()

    customerData = spark.read.option("header", "true").csv("src/test/resources/datasets/mock_ccdata.csv")
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("run should correctly detect anomalies") {
    val result = AnomalyDetectionPipeline.run(spark, customerData)
    assert(result.columns.contains("CustomerID"))
    assert(result.columns.contains("BALANCE"))
  }
}
