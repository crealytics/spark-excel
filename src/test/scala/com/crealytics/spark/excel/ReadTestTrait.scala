package com.crealytics.spark.excel

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.StructType

trait ReadTestTrait { this: BaseExcelTestSuite =>
  protected def readExcel(
    path: String,
    options: Map[String, String] = Map.empty
  ): DataFrame = {
    spark.read
      .format("com.crealytics.spark.excel")
      .options(options)
      .load(path)
  }

  protected def assertDataFrameEquals(expected: DataFrame, actual: DataFrame): Unit = {
    assert(expected.schema == actual.schema, "Schemas do not match")
    assert(expected.collect().toSet == actual.collect().toSet, "Data does not match")
  }

  protected def createDataFrame(data: Seq[Row], schema: StructType): DataFrame = {
    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  }
}
