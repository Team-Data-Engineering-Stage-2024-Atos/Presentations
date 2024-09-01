package etl.core

import org.apache.spark.sql.{DataFrame, functions => F}
import com.typesafe.config.Config
import scala.collection.JavaConverters._

object Transformer {

  // Apply operations dynamically based on the configuration
  def applyOperations(df: DataFrame, operations: Seq[Config], dfMap: Map[String, DataFrame]): DataFrame = {
    operations.foldLeft(df) { (tempDF, operationConfig) =>
      val operation = operationConfig.getString("operation")

      operation match {
        // Regular transformations
        case "uppercase" | "lowercase" | "concatenate" | "calculate_age" | "standardize" | "multiply" | "sum" | "avg" | "count" | "countDistinct" | "min" | "max" =>
          val column = operationConfig.getString("column")
          val alias = if (operationConfig.hasPath("alias")) operationConfig.getString("alias") else column

          operation match {
            case "uppercase" => tempDF.withColumn(column, F.upper(F.col(column)))
            case "lowercase" => tempDF.withColumn(column, F.lower(F.col(column)))
            case "concatenate" =>
              val params = operationConfig.getStringList("params").asScala.map(F.col).toSeq
              tempDF.withColumn(column, F.concat_ws(" ", params: _*))
            case "calculate_age" =>
              val dobCol = F.to_date(F.col(operationConfig.getStringList("params").get(0)), "yyyy-MM-dd")
              tempDF.withColumn(column, F.floor(F.datediff(F.current_date(), dobCol) / 365.25))
            case "standardize" => tempDF.withColumn(column, F.lower(F.trim(F.col(column))))
            case "multiply" =>
              val params = operationConfig.getStringList("params").asScala
              tempDF.withColumn(column, F.col(params.head) * F.lit(params(1).toDouble))
            case "sum" =>
              val groupByColumns = operationConfig.getStringList("group_by").asScala.map(F.col).toSeq
              tempDF.groupBy(groupByColumns: _*).agg(F.sum(F.col(column)).as(alias))
            case "avg" =>
              val groupByColumns = operationConfig.getStringList("group_by").asScala.map(F.col).toSeq
              tempDF.groupBy(groupByColumns: _*).agg(F.avg(F.col(column)).as(alias))
            case "count" =>
              val groupByColumns = operationConfig.getStringList("group_by").asScala.map(F.col).toSeq
              tempDF.groupBy(groupByColumns: _*).agg(F.count(F.col(column)).as(alias))
            case "countDistinct" =>
              val groupByColumns = operationConfig.getStringList("group_by").asScala.map(F.col).toSeq
              tempDF.groupBy(groupByColumns: _*).agg(F.countDistinct(F.col(column)).as(alias))
            case "min" =>
              val groupByColumns = operationConfig.getStringList("group_by").asScala.map(F.col).toSeq
              tempDF.groupBy(groupByColumns: _*).agg(F.min(F.col(column)).as(alias))
            case "max" =>
              val groupByColumns = operationConfig.getStringList("group_by").asScala.map(F.col).toSeq
              tempDF.groupBy(groupByColumns: _*).agg(F.max(F.col(column)).as(alias))
          }

        // Join operation
        case "join" =>
          val otherDF = dfMap(operationConfig.getString("other_df"))
          val joinColumns = operationConfig.getStringList("columns").asScala
          val joinType = operationConfig.getString("join_type")
          tempDF.join(otherDF, joinColumns, joinType)

        // Aggregation operation
        case "aggregate" =>
          val aggregations = operationConfig.getConfigList("aggregations").asScala
          val groupByColumns = operationConfig.getStringList("group_by").asScala.map(F.col).toSeq

          val aggExprs = aggregations.map { aggConfig =>
            val aggColumn = aggConfig.getString("column")
            val aggOperation = aggConfig.getString("operation")
            val aggAlias = if (aggConfig.hasPath("alias")) aggConfig.getString("alias") else aggColumn

            aggOperation match {
              case "sum"           => F.sum(F.col(aggColumn)).as(aggAlias)
              case "avg"           => F.avg(F.col(aggColumn)).as(aggAlias)
              case "count"         => F.count(F.col(aggColumn)).as(aggAlias)
              case "countDistinct" => F.countDistinct(F.col(aggColumn)).as(aggAlias)
              case "min"           => F.min(F.col(aggColumn)).as(aggAlias)
              case "max"           => F.max(F.col(aggColumn)).as(aggAlias)
            }
          }

          tempDF.groupBy(groupByColumns: _*).agg(aggExprs.head, aggExprs.tail: _*)

        // Dynamic calculation operation
        case "calculate" =>
          val alias = operationConfig.getString("alias")
          val expression = operationConfig.getString("expression")
          tempDF.withColumn(alias, F.expr(expression))

        case _ =>
          tempDF
      }
    }
  }

  // Handle single or multiple DataFrames and apply transformations
  def transform(input: Either[DataFrame, Map[String, DataFrame]], config: Config, targetTable: String): DataFrame = {
    val operations = config.getConfigList(s"etl.tables.$targetTable.operations").asScala

    input match {
      case Left(singleDF) =>
        // If a single DataFrame is provided, apply operations directly
        applyOperations(singleDF, operations, Map.empty)

      case Right(dfMap) =>
        // If multiple DataFrames are provided, handle joins and then apply operations
        val baseDataset = config.getStringList(s"etl.tables.$targetTable.datasets").asScala.head
        val baseDF = dfMap(baseDataset)

        val joinedDF = config.getStringList(s"etl.tables.$targetTable.datasets").asScala.tail.foldLeft(baseDF) { (leftDF, dataset) =>
          val joinConfig = config.getConfig(s"etl.tables.$targetTable.join")
          val joinColumns = joinConfig.getStringList("columns").asScala
          val joinType = joinConfig.getString("type")
          leftDF.join(dfMap(dataset), joinColumns, joinType)
        }

        applyOperations(joinedDF, operations, dfMap)
    }
  }
}
