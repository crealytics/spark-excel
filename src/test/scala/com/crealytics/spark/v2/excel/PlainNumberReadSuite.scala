/** Copyright 2016 - 2021 Martin Mauch (@nightscape)
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

import java.util

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.{Row, _}
import org.apache.spark.sql.types._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

object PlainNumberReadSuite {
  val expectedInferredSchema = StructType(
    List(
      StructField("only_numbers", DoubleType, true),
      StructField("numbers_and_text", StringType, true),
      StructField("date_formula", StringType, true)
    )
  )

  /** Breaking change with V1: Keep row will all empty cells
    * More detail: https://github.com/crealytics/spark-excel/issues/285
    */
  val expectedPlainDataInferSchema: util.List[Row] = List(
    Row(12345678901d, "12345678901-123", "12/1/20"),
    Row(123456789012d, "123456789012", "0.01"),
    Row(-0.12345678901, "0.05", "0h 14m"),
    Row(null, null, null),
    Row(Double.NaN, "abc.def", null)
  ).asJava

  /** Breaking change with V1: More data after row with all empty cells
    */
  val expectedExcelDataInferSchema: util.List[Row] = List(
    Row(1.2345678901e10, "12345678901-123", "12/1/20"),
    Row(1.23456789012e11, "1.23457E+11", "0.01"), // values are displayed in scientific notation and rounded up
    Row(-0.12345678901, "0.05", "0h 14m"),
    Row(null, null, null),
    Row(Double.NaN, "abc.def", null)
  ).asJava

  val expectedNonInferredSchema = StructType(
    List(
      StructField("only_numbers", StringType, true),
      StructField("numbers_and_text", StringType, true),
      StructField("date_formula", StringType, true)
    )
  )

  val expectedPlainDataNonInferSchema: util.List[Row] = List(
    Row("12345678901", "12345678901-123", "12/1/20"),
    Row("123456789012", "123456789012", "0.01"),
    Row("-0.12345678901", "0.05", "0h 14m"),
    Row(null, null, null),
    Row("-1/0", "abc.def", null)
  ).asJava

  val expectedExcelDataNonInferSchema: util.List[Row] = List(
    Row("12345678901", "12345678901-123", "12/1/20"),
    Row("1.23457E+11", "1.23457E+11", "0.01"), // values are displayed in scientific notation and rounded up
    Row("-0.123456789", "0.05", "0h 14m"), // values are rounded up
    Row(null, null, null),
    Row("-1/0", "abc.def", null)
  ).asJava
}

class PlainNumberReadSuite extends AnyFunSpec with DataFrameSuiteBase with Matchers {
  import PlainNumberReadSuite._

  def readFromResources(path: String, usePlainNumberFormat: Boolean, inferSchema: Boolean): DataFrame = {
    val url = getClass.getResource(path)
    spark.read
      .format("excel")
      .option("usePlainNumberFormat", usePlainNumberFormat)
      .option("inferSchema", inferSchema)
      .load(url.getPath)
  }

  describe("spark-excel-v2") {
    it("should read numbers in plain number format when usePlainNumberFormat=true and inferSchema=true") {
      val df = readFromResources("/spreadsheets/plain_number.xlsx", true, true)
      val expected = spark.createDataFrame(expectedPlainDataInferSchema, expectedInferredSchema)
      assertDataFrameEquals(expected, df)
    }

    it("should read numbers in plain number format when usePlainNumberFormat=true and inferSchema=false") {
      val df = readFromResources("/spreadsheets/plain_number.xlsx", true, false)
      val expected = spark.createDataFrame(expectedPlainDataNonInferSchema, expectedNonInferredSchema)
      assertDataFrameEquals(expected, df)
    }

    it("should read numbers in excel general number format when usePlainNumberFormat=false and inferSchema=true") {
      val df = readFromResources("/spreadsheets/plain_number.xlsx", false, true)
      val expected = spark.createDataFrame(expectedExcelDataInferSchema, expectedInferredSchema)
      assertDataFrameEquals(expected, df)
    }

    it("should read numbers in excel general number format when usePlainNumberFormat=false and inferSchema=false") {
      val df = readFromResources("/spreadsheets/plain_number.xlsx", false, false)
      val expected = spark.createDataFrame(expectedExcelDataNonInferSchema, expectedNonInferredSchema)
      assertDataFrameEquals(expected, df)
    }

  }
}
