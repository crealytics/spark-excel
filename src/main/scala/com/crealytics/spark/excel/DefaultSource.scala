package com.crealytics.spark.excel

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._


class DefaultSource
  extends RelationProvider {

  /**
    * Creates a new relation for retrieving data from an Excel file
    */
  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String]): ExcelRelation = {

    val location = checkParameter(parameters, "location")
    val sheetName = parameters.get("sheetName")
    val useHeader = checkParameter(parameters, "useHeader").toBoolean
    val treatEmptyValuesAsNulls = checkParameter(parameters, "treatEmptyValuesAsNulls").toBoolean
    val userSchema = null // checkParameter(parameters, "userSchema")
    val inferSchema = checkParameter(parameters, "inferSchema").toBoolean
    val addColorColumns = checkParameter(parameters, "addColorColumns").toBoolean
    ExcelRelation(
      location,
      sheetName,
      useHeader,
      treatEmptyValuesAsNulls,
      inferSchema,
      addColorColumns
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
