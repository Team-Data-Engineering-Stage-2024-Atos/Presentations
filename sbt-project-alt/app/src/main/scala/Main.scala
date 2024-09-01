import etl.core.ETLJob
import etl.utils.{ConfigManager, DatabaseUtils}
import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigValueFactory

object Main {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.exit(1)
    }

    val configFile = args(0)
    val outputTable = args(1)

    val config = ConfigManager.loadConfig(configFile)
      .withValue("etl.output.target_table", ConfigValueFactory.fromAnyRef(outputTable))

    val appName = config.getString("spark.appName")
    val master = config.getString("spark.master")
    val warehouse = config.getString("spark.hiveWarehouseDir")

    implicit val spark: SparkSession = SparkSession.builder()
      .appName(appName)
      .master(master)
      .config("spark.sql.warehouse.dir", warehouse)
      .enableHiveSupport()
      .getOrCreate()

    try {
      DatabaseUtils.checkAndCreateDatabase(config)
      DatabaseUtils.checkAndCreateTable(config, outputTable)
      val etlJob = new ETLJob(spark, config)
      etlJob.run()
    } finally {
      spark.stop()
    }
  }
}
