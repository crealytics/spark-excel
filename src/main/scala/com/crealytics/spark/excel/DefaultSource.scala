package com.crealytics.spark.excel

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType


class DefaultSource
  extends RelationProvider
  with SchemaRelationProvider
{

  /**
    * Creates a new relation for retrieving data from an Excel file
    */
  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String]
  ): ExcelRelation =
    createRelation(sqlContext, parameters, null)

  /**
    * Creates a new relation for retrieving data from an Excel file
    */
  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    schema: StructType
  ): ExcelRelation = {
    ExcelRelation(
      location = checkParameter(parameters, "location"),
      sheetName = parameters.get("sheetName"),
      useHeader = checkParameter(parameters, "useHeader").toBoolean,
      treatEmptyValuesAsNulls = checkParameter(parameters, "treatEmptyValuesAsNulls").toBoolean,
      userSchema = Option(schema),
      inferSheetSchema = checkParameter(parameters, "inferSchema").toBoolean,
      addColorColumns = checkParameter(parameters, "addColorColumns").toBoolean,
      startColumn = parameters.get("startColumn").fold(0)(_.toInt),
      endColumn = parameters.get("endColumn").fold(Int.MaxValue)(_.toInt)
    )(sqlContext)
  }

  // Forces a Parameter to exist, otherwise an exception is thrown.
  private def checkParameter(map: Map[String, String], param: String) = {
    if (!map.contains(param)) {
      throw new IllegalArgumentException(s"Parameter ${'"'}$param${'"'} is missing in options.")
    } else {
      map.apply(param)
    }
  }

  // Gets the Parameter if it exists, otherwise returns the default argument
  private def parameterOrDefault(map: Map[String, String], param: String, default: String) =
    map.getOrElse(param, default)
}
