package etl.utils

import org.apache.spark.sql.SparkSession
import com.typesafe.config.Config
import scala.collection.JavaConverters._

object DatabaseUtils {

  def checkAndCreateDatabase(config: Config)(implicit spark: SparkSession): Unit = {
    val database = config.getString("hive.database")
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $database")
  }

  def checkAndCreateTable(config: Config, tableName: String)(implicit spark: SparkSession): Unit = {
    val columnsConfig = config.getStringList(s"etl.tables.$tableName.columns").asScala
    val columnsDDL = columnsConfig.map(col => s"$col STRING").mkString(", ")

    val database = config.getString("hive.database")
    spark.sql(s"CREATE TABLE IF NOT EXISTS $database.$tableName ($columnsDDL)")
  }
}

