package com.crealytics.spark.excel

import java.util

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql._
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

  val expectedDataInferSchema: util.List[Row] = List(
    Row(12345678901d, "12345678901-123", "12/1/20"),
    Row(123456789012d, "123456789012", "0.01"),
    Row(-0.12345678901, "0.05", "0h 14m"),
    Row(null, null, null)
  ).asJava

  val expectedNonInferredSchema = StructType(
    List(
      StructField("only_numbers", StringType, true),
      StructField("numbers_and_text", StringType, true),
      StructField("date_formula", StringType, true)
    )
  )

  val expectedDataNonInferSchema: util.List[Row] = List(
    Row("12345678901", "12345678901-123", "12/1/20"),
    Row("123456789012", "123456789012", "0.01"),
    Row("-0.12345678901", "0.05", "0h 14m"),
    Row(null, null, null),
    Row("-1/0", "abc.def", null)
  ).asJava
}

class PlainNumberReadSuite extends AnyFunSpec with DataFrameSuiteBase with Matchers {
  import PlainNumberReadSuite._

  def readFromResources(path: String, inferSchema: Boolean): DataFrame = {
    val url = getClass.getResource(path)
    spark.read.excel(inferSchema = inferSchema).load(url.getPath)
  }

  describe("spark-excel") {
    it("should read long numbers in plain number format when inferSchema is true") {
      val df = readFromResources("/spreadsheets/plain_number.xlsx", true)
      val expected = spark.createDataFrame(expectedDataInferSchema, expectedInferredSchema)
      assertDataFrameEquals(expected, df)
    }

    it("should read long numbers in plain number format when inferSchema is false") {
      val df = readFromResources("/spreadsheets/plain_number.xlsx", false)
      val expected = spark.createDataFrame(expectedDataNonInferSchema, expectedNonInferredSchema)
      assertDataFrameEquals(expected, df)
    }

  }
}
