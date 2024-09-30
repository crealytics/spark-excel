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

package com.crealytics.spark.excel

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

import java.sql.Timestamp
import java.time.LocalDateTime
import java.util
import scala.jdk.CollectionConverters._

object ErrorsAsStringsReadSuite {
  private val dummyTimestamp = Timestamp.valueOf(LocalDateTime.of(2021, 2, 19, 0, 0))
  private val epochTimestamp = new Timestamp(0)
  private val dummyText = "hello"

  private val expectedSchemaInfer = StructType(
    List(
      StructField("double", DoubleType, true),
      StructField("boolean", BooleanType, true),
      StructField("timestamp", TimestampType, true),
      StructField("string", StringType, true),
      StructField("formula", StringType, true)
    )
  )
  private val expectedDataErrorsAsStringsInfer: util.List[Row] =
    List(
      Row(1.0, true, dummyTimestamp, dummyText, "A1"),
      Row(2.0, false, dummyTimestamp, dummyText, "A3"),
      Row(0.0, false, epochTimestamp, "", ""),
      Row(0.0, false, epochTimestamp, "", "")
    ).asJava

  private val expectedDataErrorsAsNullInfer: util.List[Row] =
    List(
      Row(1.0, true, dummyTimestamp, dummyText, "A1"),
      Row(2.0, false, dummyTimestamp, dummyText, "A3"),
      Row(null, null, null, null, null),
      Row(null, null, null, null, null)
    ).asJava

  private val expectedSchemaNonInfer = StructType(
    List(
      StructField("double", StringType, true),
      StructField("boolean", StringType, true),
      StructField("timestamp", StringType, true),
      StructField("string", StringType, true),
      StructField("formula", StringType, true)
    )
  )
  private val expectedDataErrorsAsStringsNonInfer: util.List[Row] =
    List(
      Row("1", "TRUE", "19\"-\"Feb\"-\"2021", dummyText, "A1"),
      Row("2", "FALSE", "19\"-\"Feb\"-\"2021", dummyText, "A3"),
      Row("", "", "", "", ""),
      Row("", "", "", "", "")
    ).asJava

  private val expectedDataErrorsAsNullNonInfer: util.List[Row] =
    List(
      Row("1", "TRUE", "19\"-\"Feb\"-\"2021", "hello", "A1"),
      Row("2", "FALSE", "19\"-\"Feb\"-\"2021", "hello", "A3"),
      Row(null, null, null, null, null),
      Row(null, null, null, null, null)
    ).asJava

  private val excelLocation = "/spreadsheets/with_errors_all_types.xlsx"
}

class ErrorsAsStringsReadSuite extends BaseExcelTestSuite with ReadTestTrait {
  import ErrorsAsStringsReadSuite._

  test("should read errors in string format when setErrorCellsToFallbackValues=true and inferSchema=true") {
    val df = readExcel(
      ExcelTestUtils.resourcePath(excelLocation),
      Map("setErrorCellsToFallbackValues" -> "true", "inferSchema" -> "true", "excerptSize" -> "3")
    )
    val expected = createDataFrame(expectedDataErrorsAsStringsInfer.asScala.toSeq, expectedSchemaInfer)
    assertDataFrameEquals(expected, df)
  }

  test("should read errors as null when setErrorCellsToFallbackValues=false and inferSchema=true") {
    val df = readExcel(
      ExcelTestUtils.resourcePath(excelLocation),
      Map("setErrorCellsToFallbackValues" -> "false", "inferSchema" -> "true", "excerptSize" -> "3")
    )
    val expected = createDataFrame(expectedDataErrorsAsNullInfer.asScala.toSeq, expectedSchemaInfer)
    assertDataFrameEquals(expected, df)
  }

  test("should read errors in string format when setErrorCellsToFallbackValues=true and inferSchema=false") {
    val df = readExcel(
      ExcelTestUtils.resourcePath(excelLocation),
      Map("setErrorCellsToFallbackValues" -> "true", "inferSchema" -> "false", "excerptSize" -> "3")
    )
    val expected = createDataFrame(expectedDataErrorsAsStringsNonInfer.asScala.toSeq, expectedSchemaNonInfer)
    assertDataFrameEquals(expected, df)
  }

  test("should read errors in string format when setErrorCellsToFallbackValues=false and inferSchema=false") {
    val df = readExcel(
      ExcelTestUtils.resourcePath(excelLocation),
      Map("setErrorCellsToFallbackValues" -> "false", "inferSchema" -> "false", "excerptSize" -> "3")
    )
    val expected = createDataFrame(expectedDataErrorsAsNullNonInfer.asScala.toSeq, expectedSchemaNonInfer)
    assertDataFrameEquals(expected, df)
  }
}
