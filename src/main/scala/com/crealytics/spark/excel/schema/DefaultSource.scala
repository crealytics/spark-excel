package com.crealytics.spark.excel.schema

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.SchemaRelationProvider
import org.apache.spark.sql.types.StructType
import com.crealytics.spark.excel.ExcelRelation
import com.crealytics.spark.excel.ParameterExtractor

class DefaultSource
  extends SchemaRelationProvider
  with ParameterExtractor{

  /**
    * Creates a new relation for retrieving data from an Excel file
    */
  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String],
                               schema: StructType): ExcelRelation = {
    implicit def context = sqlContext
    implicit def schemaOpt: Option[StructType] = Some(schema)
    ExcelRelation.createExcelRelation(parameters)
  }
}
