package com.crealytics.spark.excel

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.SchemaRelationProvider
import org.apache.spark.sql.types.StructType


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
    implicit def schemaOpt: Option[StructType] =
      if (schema == null) None else Some(schema)
    ExcelRelation.createExcelRelation(parameters)
  }

}
