package com.crealytics.spark.excel

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

  val expectedPlainDataInferSchema: util.List[Row] = List(
    Row(12345678901d, "12345678901-123", "12/1/20"),
    Row(123456789012d, "123456789012", "0.01"),
    Row(-0.12345678901, "0.05", "0h 14m"),
    Row(null, null, null),
    Row(null, "abc.def", null)
  ).asJava

  val expectedExcelDataInferSchema: util.List[Row] = List(
    Row(1.2345678901e10, "12345678901-123", "12/1/20"),
    Row(1.23456789012e11, "1.23457E+11", "0.01"), // values are displayed in scientific notation and rounded up
    Row(-0.12345678901, "0.05", "0h 14m"),
    Row(null, null, null),
    Row(null, "abc.def", null)
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
    Row(null, "abc.def", null)
  ).asJava

  val expectedExcelDataNonInferSchema: util.List[Row] = List(
    Row("12345678901", "12345678901-123", "12/1/20"),
    Row("1.23457E+11", "1.23457E+11", "0.01"), // values are displayed in scientific notation and rounded up
    Row("-0.123456789", "0.05", "0h 14m"), // values are rounded up
    Row(null, null, null),
    Row(null, "abc.def", null)
  ).asJava
}

class PlainNumberReadSuite extends AnyFunSpec with DataFrameSuiteBase with Matchers {
  import PlainNumberReadSuite._

  def readFromResources(path: String, usePlainNumberFormat: Boolean, inferSchema: Boolean): DataFrame = {
    val url = getClass.getResource(path)
    spark.read.excel(usePlainNumberFormat = usePlainNumberFormat, inferSchema = inferSchema).load(url.getPath)
  }

  describe("spark-excel") {
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
