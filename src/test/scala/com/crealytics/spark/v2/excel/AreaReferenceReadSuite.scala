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

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

import java.util
import scala.jdk.CollectionConverters._

/** Loading data from difference data address (AreaReference)
  */
object AreaReferenceReadSuite {
  val expectedSchema_01 = StructType(
    List(
      StructField("Translations!$A$370", StringType, true),
      StructField("Translations!$A$371", LongType, true),
      StructField("Translations!$A$402", DoubleType, true),
      StructField("Translations!$A$393", DoubleType, true),
      StructField("Translations!$A$384", DoubleType, true),
      StructField("Translations!$A$405", DoubleType, true),
      StructField("Translations!$A$396", DoubleType, true),
      StructField("Translations!$A$387", DoubleType, true),
      StructField("Translations!$A$418", DoubleType, true),
      StructField("Translations!$A$419", DoubleType, true),
      StructField("Translations!$A$4110", DoubleType, true)
    )
  )

  /* Manually checking 1 row only */
  val expectedData_01: util.List[Row] = List(
    Row(
      "Alabama",
      140895441L,
      458d,
      122d,
      85116d,
      1009700176.36684d,
      268959435.626102d,
      187645502645.503d,
      0.0072d,
      0.0019d,
      1.3318d
    )
  ).asJava

}

class AreaReferenceReadSuite extends AnyFunSuite with DataFrameSuiteBase with ExcelTestingUtilities {
  import AreaReferenceReadSuite._

  test("AreaReference from diffrence sheet with testing data from Apache POI upstream tests") {
    val df = readFromResources(
      spark,
      path = "apache_poi/57231_MixedGasReport.xls",
      options = Map("dataAddress" -> "'Coefficient Table'!A6", "ignoreAfterHeader" -> 2, "inferSchema" -> true)
    ).limit(1)
    val expected = spark.createDataFrame(expectedData_01, expectedSchema_01)
    assertDataFrameApproximateEquals(expected, df, 0.1e-2)
  }

}
