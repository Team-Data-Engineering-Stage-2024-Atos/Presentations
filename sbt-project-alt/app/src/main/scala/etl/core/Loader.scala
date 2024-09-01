package etl.core

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import com.typesafe.config.Config
import scala.util.{Try, Success, Failure}

object Loader {
  def loadData(df: DataFrame, targetTable: String, columns: Seq[String], config: Config)(implicit spark: SparkSession): Unit = {
    val database = config.getString("hive.database")
    val fullTableName = s"$database.$targetTable"

    val maxRetries = 5
    var attempt = 0
    var success = false

    while (attempt < maxRetries && !success) {
      attempt += 1
      Try {
        df.selectExpr(columns: _*)
          .write
          .mode(SaveMode.Overwrite)
          .format("hive")
          .saveAsTable(fullTableName)
      } match {
        case Success(_) =>
          success = true
          println(s"Data successfully loaded into table $fullTableName on attempt $attempt.")

        case Failure(exception) =>
          println(s"Attempt $attempt to load data into table $fullTableName failed: ${exception.getMessage}")
          if (attempt < maxRetries) {
            println("Retrying...")
            Thread.sleep(2000) // wait for 2 seconds before retrying
          } else {
            println(s"Failed to load data into table $fullTableName after $maxRetries attempts.")
            throw exception
          }
      }
    }
  }
}
