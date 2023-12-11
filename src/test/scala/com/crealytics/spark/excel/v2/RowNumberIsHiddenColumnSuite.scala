/*
 * Copyright 2022 Martin Mauch (@nightscape)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.crealytics.spark.excel.v2

import com.crealytics.spark.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

import java.util
import scala.jdk.CollectionConverters._

/** Related issues: #40 Allow reading only a subset of rows https://github.com/crealytics/spark-excel/issues/40 #59 Rows
  * are returned in incorrect order on cluster https://github.com/crealytics/spark-excel/issues/59 #115 Add excel row
  * number column https://github.com/crealytics/spark-excel/issues/115 #749 Add excel row
  * is hidden column https://github.com/crealytics/spark-excel/issues/749
  */
object RowNumberIsHiddenColumnSuite {

  val expectedSchema = StructType(
    List(
      StructField("RowIsHidden", BooleanType, true),
      StructField("RowID", IntegerType, true),
      StructField("1", StringType, true),
      StructField("2", StringType, true),
      StructField("3", StringType, true)
    )
  )

  val expectedData_NoKeep: util.List[Row] = List(
    Row(false, 0, "File info", null, null),
    Row(false, 1, "Info", "Info", "Info"),
    Row(false, 3, "Metadata", null, null),
    Row(false, 5, null, "1", "2"),
    Row(true, 6, "A", "1", "2"),
    Row(false, 7, "B", "5", "6"),
    Row(false, 8, "C", "9", "10"),
    Row(false, 11, "Metadata", null, null),
    Row(false, 13, null, "1", "2"),
    Row(false, 14, "A", "1", "2"),
    Row(false, 15, "B", "4", "5"),
    Row(true, 16, "C", "7", "8")
  ).asJava

  val expectedData_Keep: util.List[Row] = List(
    Row(false, 0, "File info", null, null),
    Row(false, 1, "Info", "Info", "Info"),
    Row(null, null, null, null, null),
    Row(false, 3, "Metadata", null, null),
    Row(null, null, null, null, null),
    Row(false, 5, null, "1", "2"),
    Row(true, 6, "A", "1", "2"),
    Row(false, 7, "B", "5", "6"),
    Row(false, 8, "C", "9", "10"),
    Row(null, null, null, null, null),
    Row(null, null, null, null, null),
    Row(false, 11, "Metadata", null, null),
    Row(null, null, null, null, null),
    Row(false, 13, null, "1", "2"),
    Row(false, 14, "A", "1", "2"),
    Row(false, 15, "B", "4", "5"),
    Row(true, 16, "C", "7", "8")
  ).asJava

  val expectedSchema_Projection = StructType(
    List(
      StructField("RowIsHidden", BooleanType, true),
      StructField("3", StringType, true),
      StructField("RowID", IntegerType, true),
      StructField("2", StringType, true)
    )
  )

  val expectedData_Projection: util.List[Row] = List(
    Row(false, null, 0, null),
    Row(false, "Info", 1, "Info"),
    Row(false, null, 3, null),
    Row(false, "2", 5, "1"),
    Row(true, "2", 6, "1"),
    Row(false, "6", 7, "5"),
    Row(false, "10", 8, "9"),
    Row(false, null, 11, null),
    Row(false, "2", 13, "1"),
    Row(false, "2", 14, "1"),
    Row(false, "5", 15, "4"),
    Row(true, "8", 16, "7")
  ).asJava

}

class RowNumberIsHiddenColumnSuite extends AnyFunSuite with DataFrameSuiteBase with ExcelTestingUtilities {
  import RowNumberIsHiddenColumnSuite._

  test("read with addition excel row number column") {
    val df = readFromResources(
      spark,
      path = "issue_749_max.xlsx",
      Map("header" -> false, "keepUndefinedRows" -> false,
        "columnNameOfRowNumber" -> "RowID", "columnNameOfRowIsHidden" -> "RowIsHidden"),
      schema = expectedSchema
    )
    val expected = spark.createDataFrame(expectedData_NoKeep, expectedSchema)
    assertDataFrameEquals(expected, df)
  }

  test("read with addition excel row number column, keep undefined rows") {
    val df = readFromResources(
      spark,
      path = "/issue_749_max.xlsx",
      Map("header" -> false, "keepUndefinedRows" -> true,
        "columnNameOfRowNumber" -> "RowID", "columnNameOfRowIsHidden" -> "RowIsHidden"),
      schema = expectedSchema
    )
    val expected = spark.createDataFrame(expectedData_Keep, expectedSchema)
    assertDataFrameEquals(expected, df)
  }

  test("read with addition excel row number column, projection") {
    val df = readFromResources(
      spark,
      path = "/issue_749_max.xlsx",
      Map("header" -> false, "keepUndefinedRows" -> false,
        "columnNameOfRowNumber" -> "RowID", "columnNameOfRowIsHidden" -> "RowIsHidden"),
      schema = expectedSchema
    ).select("RowIsHidden", "3", "RowID", "2")
    val expected = spark.createDataFrame(expectedData_Projection, expectedSchema_Projection)
    assertDataFrameEquals(expected, df)
  }
}
