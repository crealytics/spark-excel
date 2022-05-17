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
import scala.collection.JavaConverters._

object InferStricterNumericalTypesSuite {
  val expectedInferredSchema = StructType(
    List(
      StructField("ID", StringType, true),
      StructField("Integer Value Range", IntegerType, true),
      StructField("Long Value Range", LongType, true),
      StructField("Double Value Range", DoubleType, true)
    )
  )

  /** Stricter type for numerical value
    */
  val expectedDataInferSchema: util.List[Row] = List(
    Row("Gas & Oil", 2147482967, 92147483647L, 90315085.71d),
    Row("Telecomunication", 2147483099, 102147483647L, -965079398.74d),
    Row("Manufacturing", 2147482826, 112147483647L, -353020871.56d),
    Row("Farming", 2147482838, -102147483647L, -446026564.15d),
    Row("Service", 2147483356, -112147483647L, -820766945.73d)
  ).asJava
}

class InferStricterNumericalTypesSuite extends AnyFunSuite with DataFrameSuiteBase with ExcelTestingUtilities {
  import InferStricterNumericalTypesSuite._

  test("stricter numerical types usePlainNumberFormat=true and inferSchema=true (xlxs)") {
    val df = readFromResources(
      spark,
      path = "infer_stricter_numerical_types.xlsx",
      options = Map("usePlainNumberFormat" -> true, "inferSchema" -> true)
    )
    val expected = spark.createDataFrame(expectedDataInferSchema, expectedInferredSchema)
    assertDataFrameEquals(expected, df)
  }

  test("stricter numerical types usePlainNumberFormat=false and inferSchema=true (xlxs)") {
    val df = readFromResources(
      spark,
      path = "infer_stricter_numerical_types.xlsx",
      options = Map("usePlainNumberFormat" -> false, "inferSchema" -> true)
    )
    val expected = spark.createDataFrame(expectedDataInferSchema, expectedInferredSchema)
    assertDataFrameEquals(expected, df)
  }

  test("stricter numerical types usePlainNumberFormat=true and inferSchema=true (xls)") {
    val df = readFromResources(
      spark,
      path = "infer_stricter_numerical_types.xls",
      options = Map("usePlainNumberFormat" -> true, "inferSchema" -> true)
    )
    val expected = spark.createDataFrame(expectedDataInferSchema, expectedInferredSchema)
    assertDataFrameEquals(expected, df)
  }

  test("stricter numerical types usePlainNumberFormat=false and inferSchema=true (xls)") {
    val df = readFromResources(
      spark,
      path = "infer_stricter_numerical_types.xls",
      options = Map("usePlainNumberFormat" -> false, "inferSchema" -> true)
    )
    val expected = spark.createDataFrame(expectedDataInferSchema, expectedInferredSchema)
    assertDataFrameEquals(expected, df)
  }
}
