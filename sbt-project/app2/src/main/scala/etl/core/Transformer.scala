package etl.core

import org.apache.spark.sql.{DataFrame, functions => F}
import com.typesafe.config.Config

object Transformer {
  def applyTransformations(df: DataFrame, transformations: Seq[Config]): DataFrame = {
    transformations.foldLeft(df) { (tempDF, trans) =>
      val column = trans.getString("column")
      val operation = trans.getString("operation")
      operation match {
        case "uppercase" => tempDF.withColumn(column, F.upper(F.col(column)))
        case "lowercase" => tempDF.withColumn(column, F.lower(F.col(column)))
        case _ => tempDF
      }
    }
  }
}
