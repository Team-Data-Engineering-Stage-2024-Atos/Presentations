package etl.utils

import java.sql.{Connection, DriverManager, SQLException, Statement}
import com.typesafe.config.Config
import scala.collection.JavaConverters._


object DatabaseUtils {
  def checkAndCreateDatabase(config: Config): Unit = {
    val jdbcUrl = s"jdbc:postgresql://${config.getString("db.host")}:${config.getInt("db.port")}/postgres"
    val database = config.getString("db.database")
    val user = config.getString("db.username")
    val password = config.getString("db.password")

    var connection: Connection = null
    var statement: Statement = null
    try {
      connection = DriverManager.getConnection(jdbcUrl, user, password)
      statement = connection.createStatement()
      val resultSet = statement.executeQuery(s"SELECT 1 FROM pg_database WHERE datname = '$database'")
      if (!resultSet.next()) {
        statement.executeUpdate(s"CREATE DATABASE $database")
      }
    } catch {
      case e: SQLException =>
        throw e
    } finally {
      if (statement != null) statement.close()
      if (connection != null) connection.close()
    }
  }

  def checkAndCreateTable(config: Config, tableName: String): Unit = {
    val jdbcUrl = s"jdbc:postgresql://${config.getString("db.host")}:${config.getInt("db.port")}/${config.getString("db.database")}"
    val user = config.getString("db.username")
    val password = config.getString("db.password")

    val columnsConfig = config.getStringList(s"etl.tables.$tableName.columns").asScala
    val columnsDDL = columnsConfig.map(col => s"$col TEXT").mkString(", ")

    val createTableSQL = s"CREATE TABLE IF NOT EXISTS $tableName ($columnsDDL)"

    executeSQL(jdbcUrl, user, password, createTableSQL)
  }

  private def executeSQL(jdbcUrl: String, user: String, password: String, sql: String): Unit = {
    var connection: Connection = null
    var statement: Statement = null
    try {
      connection = DriverManager.getConnection(jdbcUrl, user, password)
      statement = connection.createStatement()
      statement.executeUpdate(sql)
    } catch {
      case e: SQLException =>
        throw e
    } finally {
      if (statement != null) statement.close()
      if (connection != null) connection.close()
    }
  }
}
