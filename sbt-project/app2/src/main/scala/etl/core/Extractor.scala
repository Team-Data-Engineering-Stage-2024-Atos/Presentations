package etl.core

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.typesafe.config.Config

object Extractor {
  def extractData(spark: SparkSession, config: Config): DataFrame = {
    val format = config.getString("etl.input.format")
    val inputName = config.getString("etl.output.target_table").replaceAll("\\b\\w+_", "")
    val inputPath = config.getString("etl.input.path") + s"$inputName" + s".$format"
    
    spark.read.format(format).option("header", "true").load(inputPath)
  }
}
