package com.crealytics.spark.excel

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.RelationProvider
import org.apache.spark.sql.types.StructType


class DefaultSource
  extends RelationProvider 
  with ParameterExtractor{

  /**
    * Creates a new relation for retrieving data from an Excel file
    */
  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String]): ExcelRelation = {
    implicit def context = sqlContext
    implicit def schemaOpt: Option[StructType] = None
    ExcelRelation.createExcelRelation(parameters)
  }

}
