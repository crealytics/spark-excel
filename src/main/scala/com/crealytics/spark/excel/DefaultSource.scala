package com.crealytics.spark.excel

import com.crealytics.spark.excel.Utils._
import org.apache.hadoop.fs.Path
import org.apache.poi.ss.util.{CellRangeAddress, CellReference}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.util.Try

class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider {

  /**
    * Creates a new relation for retrieving data from an Excel file
    */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): ExcelRelation =
    createRelation(sqlContext, parameters, null)

  /**
    * Creates a new relation for retrieving data from an Excel file
    */
  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    schema: StructType
  ): ExcelRelation = {
    val wbReader = WorkbookReader(parameters, sqlContext.sparkContext.hadoopConfiguration)
    val dataLocator = DataLocator(parameters)
    ExcelRelation(
      header = checkParameter(parameters, "header").toBoolean,
      treatEmptyValuesAsNulls = parameters.get("treatEmptyValuesAsNulls").fold(false)(_.toBoolean),
      userSchema = Option(schema),
      inferSheetSchema = parameters.get("inferSchema").fold(false)(_.toBoolean),
      addColorColumns = parameters.get("addColorColumns").fold(false)(_.toBoolean),
      timestampFormat = parameters.get("timestampFormat"),
      excerptSize = parameters.get("excerptSize").fold(10)(_.toInt),
      dataLocator = dataLocator,
      workbookReader = wbReader
    )(sqlContext)
  }

  override def createRelation(
    sqlContext: SQLContext,
    mode: SaveMode,
    parameters: Map[String, String],
    data: DataFrame
  ): BaseRelation = {
    val path = checkParameter(parameters, "path")
    val header = checkParameter(parameters, "header").toBoolean
    val filesystemPath = new Path(path)
    val fs = filesystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
    new ExcelFileSaver(
      fs,
      filesystemPath,
      data,
      saveMode = mode,
      header = header,
      dataLocator = DataLocator(parameters)
    ).save()

    createRelation(sqlContext, parameters, data.schema)
  }

  // Forces a Parameter to exist, otherwise an exception is thrown.
  private def checkParameter(map: Map[String, String], param: String): String = {
    if (!map.contains(param)) {
      throw new IllegalArgumentException(s"Parameter ${'"'}$param${'"'} is missing in options.")
    } else {
      map.apply(param)
    }
  }
}
