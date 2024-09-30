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

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

import scala.jdk.CollectionConverters._

object PlainNumberReadSuite {
  // Keep the existing object content as is
}

class PlainNumberReadSuite extends BaseExcelTestSuite with ReadTestTrait {
  import PlainNumberReadSuite._
  import ExcelTestUtils.resourcePath

  def readFromResources(path: String, usePlainNumberFormat: Boolean, inferSchema: Boolean): DataFrame = {
    readExcel(
      resourcePath(path),
      Map("usePlainNumberFormat" -> usePlainNumberFormat.toString, "inferSchema" -> inferSchema.toString)
    )
  }

  test("should read numbers in plain number format when usePlainNumberFormat=true and inferSchema=true") {
    val df = readFromResources("/spreadsheets/plain_number.xlsx", true, true)
    val expected = createDataFrame(expectedPlainDataInferSchema.asScala.toSeq, expectedInferredSchema)
    assertDataFrameEquals(expected, df)
  }

  test("should read numbers in plain number format when usePlainNumberFormat=true and inferSchema=false") {
    val df = readFromResources("/spreadsheets/plain_number.xlsx", true, false)
    val expected = createDataFrame(expectedPlainDataNonInferSchema.asScala.toSeq, expectedNonInferredSchema)
    assertDataFrameEquals(expected, df)
  }

  test("should read numbers in excel general number format when usePlainNumberFormat=false and inferSchema=true") {
    val df = readFromResources("/spreadsheets/plain_number.xlsx", false, true)
    val expected = createDataFrame(expectedExcelDataInferSchema.asScala.toSeq, expectedInferredSchema)
    assertDataFrameEquals(expected, df)
  }

  test("should read numbers in excel general number format when usePlainNumberFormat=false and inferSchema=false") {
    val df = readFromResources("/spreadsheets/plain_number.xlsx", false, false)
    val expected = createDataFrame(expectedExcelDataNonInferSchema.asScala.toSeq, expectedNonInferredSchema)
    assertDataFrameEquals(expected, df)
  }
}
