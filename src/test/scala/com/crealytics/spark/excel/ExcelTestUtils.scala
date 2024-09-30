package com.crealytics.spark.excel

import org.apache.spark.sql.types._

object ExcelTestUtils {
  def resourcePath(path: String): String = {
    getClass.getResource(path).getPath
  }

  def sampleSchema: StructType = StructType(Seq(
    StructField("id", IntegerType),
    StructField("name", StringType),
    StructField("value", DoubleType)
  ))

  def sampleData: Seq[Seq[Any]] = Seq(
    Seq(1, "Alice", 10.5),
    Seq(2, "Bob", 20.0),
    Seq(3, "Charlie", 30.5)
  )
}
