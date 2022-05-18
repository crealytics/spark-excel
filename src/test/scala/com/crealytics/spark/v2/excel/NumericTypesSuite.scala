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

package com.crealytics.spark.v2.excel

import com.crealytics.spark.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

import java.util
import scala.jdk.CollectionConverters._

/** For schema infering as well as loading for various numeric types {Integer, Long, Double}
  */
object NumericTypesSuite {

  val userDefinedSchema_01 = StructType(
    List(
      StructField("Day", IntegerType, true),
      StructField("Month", IntegerType, true),
      StructField("Customer ID", StringType, true),
      StructField("Customer Name", StringType, true),
      StructField("Standard Package", IntegerType, true),
      StructField("Extra Option 1", IntegerType, true),
      StructField("Extra Option 2", IntegerType, true),
      StructField("Extra Option 3", IntegerType, true),
      StructField("Staff", StringType, true)
    )
  )

  val expectedData_01: util.List[Row] = List(
    Row(1, 12, "CA869", "Phạm Uyển Trinh", null, null, 2200, null, "Ella Fitzgerald"),
    Row(1, 12, "CA870", "Nguyễn Liên Thảo", null, null, 2000, 1350, "Ella Fitzgerald"),
    Row(1, 12, "CA871", "Lê Thị Nga", 17000, null, null, null, "Ella Fitzgerald"),
    Row(1, 12, "CA872", "Phan Tố Nga", null, null, 2000, null, "Teresa Teng"),
    Row(1, 12, "CA873", "Nguyễn Thị Teresa Teng", null, null, 1200, null, "Jesse Thomas")
  ).asJava

  val userDefinedSchema_02 = StructType(
    List(
      StructField("Day", LongType, true),
      StructField("Month", LongType, true),
      StructField("Customer ID", StringType, true),
      StructField("Customer Name", StringType, true),
      StructField("Standard Package", IntegerType, true),
      StructField("Extra Option 1", IntegerType, true),
      StructField("Extra Option 2", IntegerType, true),
      StructField("Extra Option 3", LongType, true),
      StructField("Staff", StringType, true)
    )
  )

  val expectedData_02: util.List[Row] = List(
    Row(1L, 12L, "CA869", "Phạm Uyển Trinh", null, null, 2200, null, "Ella Fitzgerald"),
    Row(1L, 12L, "CA870", "Nguyễn Liên Thảo", null, null, 2000, 1350L, "Ella Fitzgerald"),
    Row(1L, 12L, "CA871", "Lê Thị Nga", 17000, null, null, null, "Ella Fitzgerald"),
    Row(1L, 12L, "CA872", "Phan Tố Nga", null, null, 2000, null, "Teresa Teng"),
    Row(1L, 12L, "CA873", "Nguyễn Thị Teresa Teng", null, null, 1200, null, "Jesse Thomas")
  ).asJava
}

class NumericTypesSuite extends AnyFunSuite with DataFrameSuiteBase with ExcelTestingUtilities {
  import NumericTypesSuite._

  test("load with user defined schema with Integer types") {
    val df = readFromResources(
      spark,
      path = "ca_dataset/2019/Quarter=4/ca_12.xlsx",
      options = Map("header" -> true),
      schema = userDefinedSchema_01
    ).limit(5)
    val expected = spark.createDataFrame(expectedData_01, userDefinedSchema_01)

    assertDataFrameEquals(expected, df)
  }

  test("load with user defined schema with both Integer and Long types") {
    val df = readFromResources(
      spark,
      path = "ca_dataset/2019/Quarter=4/ca_12.xlsx",
      options = Map("header" -> true),
      schema = userDefinedSchema_02
    ).limit(5)
    val expected = spark.createDataFrame(expectedData_02, userDefinedSchema_02)

    assertDataFrameEquals(expected, df)
  }
}
