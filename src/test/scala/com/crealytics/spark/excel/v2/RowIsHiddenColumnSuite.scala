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
  * are returned in incorrect order on cluster https://github.com/crealytics/spark-excel/issues/59 #749 Add excel row
  * is hidden column https://github.com/crealytics/spark-excel/issues/749
  */
object RowIsHiddenColumnSuite {

  val expectedSchema = StructType(
    List(
      StructField("RowIsHidden", BooleanType, true),
      StructField("1", StringType, true),
      StructField("2", StringType, true),
      StructField("3", StringType, true)
    )
  )

  val expectedData_NoKeep: util.List[Row] = List(
    Row(false, "File info", null, null),
    Row(false, "Info", "Info", "Info"),
    Row(false, "Metadata", null, null),
    Row(false, null, "1", "2"),
    Row(true, "A", "1", "2"),
    Row(false, "B", "5", "6"),
    Row(false, "C", "9", "10"),
    Row(false, "Metadata", null, null),
    Row(false, null, "1", "2"),
    Row(false, "A", "1", "2"),
    Row(false, "B", "4", "5"),
    Row(true, "C", "7", "8")
  ).asJava

  val expectedSchema_Projection = StructType(
    List(
      StructField("3", StringType, true),
      StructField("RowIsHidden", BooleanType, true),
      StructField("2", StringType, true)
    )
  )

  val expectedData_Projection: util.List[Row] = List(
    Row(null, false, null),
    Row("Info", false, "Info"),
    Row(null, false, null),
    Row("2", false, "1"),
    Row("2", true, "1"),
    Row("6", false, "5"),
    Row("10", false, "9"),
    Row(null, false, null),
    Row("2", false, "1"),
    Row("2", false, "1"),
    Row("5", false, "4"),
    Row("8", true, "7")
  ).asJava

}

class RowIsHiddenColumnSuite extends AnyFunSuite with DataFrameSuiteBase with ExcelTestingUtilities {
  import RowIsHiddenColumnSuite._

  test("read with addition excel row is hidden column") {
    val df = readFromResources(
      spark,
      path = "issue_749_max.xlsx",
      Map("header" -> false, "keepUndefinedRows" -> false, "columnNameOfRowIsHidden" -> "RowIsHidden"),
      schema = expectedSchema
    )
    val expected = spark.createDataFrame(expectedData_NoKeep, expectedSchema)
    assertDataFrameEquals(expected, df)
  }

  test("read with addition excel row is hidden column, projection") {
    val df = readFromResources(
      spark,
      path = "/issue_749_max.xlsx",
      Map("header" -> false, "keepUndefinedRows" -> false, "columnNameOfRowIsHidden" -> "RowIsHidden"),
      schema = expectedSchema
    ).select("3", "RowIsHidden", "2")
    val expected = spark.createDataFrame(expectedData_Projection, expectedSchema_Projection)
    assertDataFrameEquals(expected, df)
  }
}
