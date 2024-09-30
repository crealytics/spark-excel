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

import com.crealytics.spark.excel.{BaseExcelTestSuite, ReadTestTrait, ExcelTestUtils}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.sql.Timestamp
import java.time.LocalDateTime
import scala.jdk.CollectionConverters._

object ErrorsAsStringsReadSuite {
  // Keep the existing object content as is
}

/** Breaking change with V1: For Spark String Type field, Error Cell has an option to either get error value or null as
  * any other Spark Types
  *
  * Related issues: Support ERROR cell type when using inferSchema=true link:
  * https://github.com/crealytics/spark-excel/pull/343
  */
class ErrorsAsStringsReadSuite extends BaseExcelTestSuite with ReadTestTrait {
  import ErrorsAsStringsReadSuite._
  import ExcelTestUtils.resourcePath

  test("error cells as null when useNullForErrorCells=true and inferSchema=true") {
    val df = readExcel(
      path = resourcePath("/with_errors_all_types.xlsx"),
      options = Map("inferSchema" -> "true", "useNullForErrorCells" -> "true")
    )
    val expected = createDataFrame(expectedDataErrorsAsNullInfer.asScala.toSeq, expectedSchemaInfer)
    assertDataFrameEquals(expected, df)
  }

  test("errors as null for non-string type with useNullForErrorCells=false and inferSchema=true") {
    val df = readExcel(
      path = resourcePath("/with_errors_all_types.xlsx"),
      options = Map("inferSchema" -> "true", "useNullForErrorCells" -> "false")
    )
    val expected = createDataFrame(expectedDataErrorsAsStringsInfer.asScala.toSeq, expectedSchemaInfer)
    assertDataFrameEquals(expected, df)
  }

  test("errors in string format when useNullForErrorCells=true and inferSchema=false") {
    val df = readExcel(
      path = resourcePath("/with_errors_all_types.xlsx"),
      options = Map("inferSchema" -> "false", "useNullForErrorCells" -> "true")
    )
    val expected = createDataFrame(expectedDataErrorsAsNullNonInfer.asScala.toSeq, expectedSchemaNonInfer)
    assertDataFrameEquals(expected, df)
  }

  test("errors in string format when useNullForErrorCells=false and inferSchema=false") {
    val df = readExcel(
      path = resourcePath("/with_errors_all_types.xlsx"),
      options = Map("inferSchema" -> "false", "useNullForErrorCells" -> "false")
    )
    val expected = createDataFrame(expectedDataErrorsAsStringsNonInfer.asScala.toSeq, expectedSchemaNonInfer)
    assertDataFrameEquals(expected, df)
  }
}
