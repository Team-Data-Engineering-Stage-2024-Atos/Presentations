package etl.core

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.typesafe.config.Config
import scala.collection.JavaConverters._

object Extractor {
  def extractData(spark: SparkSession, config: Config): Map[String, DataFrame] = {
    val targetTable = config.getString("etl.output.target_table")
    val datasets = config.getStringList(s"etl.tables.$targetTable.datasets").asScala


    println(datasets)

    datasets.map { dataset =>
      val format = config.getString("etl.input.format")
      val inputPath = s"${config.getString("etl.input.path")}$dataset"
      dataset -> spark.read.format(format).option("header", "true").load(inputPath)
    }.toMap
  }
}
