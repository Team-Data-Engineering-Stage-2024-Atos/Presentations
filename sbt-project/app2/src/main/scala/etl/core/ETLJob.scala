package etl.core

import org.apache.spark.sql.SparkSession
import com.typesafe.config.Config
import scala.collection.JavaConverters._

class ETLJob(spark: SparkSession, config: Config) {
  def run(): Unit = {
    val targetTable = config.getString("etl.output.target_table")
    val columns = config.getStringList(s"etl.tables.$targetTable.columns").asScala

    // Extract datasets as a map of DataFrames
    val dfMap = Extractor.extractData(spark, config)

    // If there is only one dataset, pass it as a single DataFrame
    val input = if (dfMap.size == 1) Left(dfMap.head._2) else Right(dfMap)

    // Perform transformations and aggregations
    val finalDF = Transformer.transform(input, config, targetTable)

    // Load the final DataFrame into the database (not implemented in Transformer)
    Loader.loadData(finalDF, targetTable, columns, config)
  }
}
