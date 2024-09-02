package etl.utils

import com.typesafe.config.{Config, ConfigFactory}

object ConfigManager {
  def loadConfig(configFile: String): Config = {
    ConfigFactory.parseFile(new java.io.File(configFile)).resolve()
  }
}
