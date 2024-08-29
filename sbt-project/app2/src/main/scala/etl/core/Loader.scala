package etl.core

import org.apache.spark.sql.{DataFrame, SaveMode}
import com.typesafe.config.Config

object Loader {
  def loadData(df: DataFrame, targetTable: String, columns: Seq[String], config: Config): Unit = {
    val jdbcUrl = s"jdbc:postgresql://${config.getString("db.host")}:${config.getInt("db.port")}/${config.getString("db.database")}"
    val user = config.getString("db.username")
    val password = config.getString("db.password")

    df.selectExpr(columns: _*)
      .write
      .mode(SaveMode.Overwrite)
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", targetTable)
      .option("user", user)
      .option("password", password)
      .save()
  }
}
