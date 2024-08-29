package etl.core

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.typesafe.config.Config
import scala.collection.JavaConverters._

class ETLJob(spark: SparkSession, config: Config) {
  def run(): Unit = {
    val targetTable = config.getString("etl.output.target_table")
    val columns = config.getStringList(s"etl.tables.$targetTable.columns").asScala
    val transformations = config.getConfigList(s"etl.tables.$targetTable.transformations").asScala

    val inputDF = Extractor.extractData(spark, config)
    val transformedDF = Transformer.applyTransformations(inputDF, transformations)
    Loader.loadData(transformedDF, targetTable, columns, config)
  }
}
