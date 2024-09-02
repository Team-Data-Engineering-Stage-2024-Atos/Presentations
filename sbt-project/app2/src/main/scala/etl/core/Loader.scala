package etl.core

import org.apache.spark.sql.{DataFrame, SaveMode}
import com.typesafe.config.Config
import scala.util.{Try, Success, Failure}

object Loader {
  def loadData(df: DataFrame, targetTable: String, columns: Seq[String], config: Config): Unit = {
    val jdbcUrl = s"jdbc:postgresql://${config.getString("db.host")}:${config.getInt("db.port")}/${config.getString("db.database")}"
    val user = config.getString("db.username")
    val password = config.getString("db.password")

    val maxRetries = 5
    var attempt = 0
    var success = false

    while (attempt < maxRetries && !success) {
      attempt += 1
      Try {
        df.selectExpr(columns: _*)
          .write
          .mode(SaveMode.Overwrite)
          .format("jdbc")
          .option("url", jdbcUrl)
          .option("dbtable", targetTable)
          .option("user", user)
          .option("password", password)
          .save()
      } match {
        case Success(_) =>
          success = true
          println(s"Data successfully loaded into table $targetTable on attempt $attempt.")

        case Failure(exception) =>
          println(s"Attempt $attempt to load data into table $targetTable failed: ${exception.getMessage}")
          if (attempt < maxRetries) {
            println("Retrying...")
            Thread.sleep(2000) // wait for 2 seconds before retrying
          } else {
            println(s"Failed to load data into table $targetTable after $maxRetries attempts.")
            throw exception
          }
      }
    }
  }
}
