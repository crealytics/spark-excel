package com.crealytics.spark.excel.executor

import com.crealytics.spark.excel.utils.{ExcelCreatableRelationProvider, ParameterChecker}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

class DefaultSource
  extends ExcelCreatableRelationProvider
    with DataSourceRegister {
  override def shortName(): String = "large_excel"

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              userSchema: StructType
                             ): ExcelExecutorRelation = ExcelExecutorRelation(
    location = ParameterChecker.check(parameters, "path"),
    sheetName = parameters.get("sheetName"),
    useHeader = ParameterChecker.check(parameters, "useHeader").toBoolean,
    treatEmptyValuesAsNulls = parameters.get("treatEmptyValuesAsNulls").fold(true)(_.toBoolean),
    schema = userSchema,
    startColumn = parameters.get("startColumn").fold(0)(_.toInt),
    endColumn = parameters.get("endColumn").fold(Int.MaxValue)(_.toInt)
  )(sqlContext)
}
