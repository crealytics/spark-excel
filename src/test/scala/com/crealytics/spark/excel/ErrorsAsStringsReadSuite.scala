package com.crealytics.spark.excel

import java.sql.Timestamp
import java.time.Instant
import java.util

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.{Row, _}
import org.apache.spark.sql.types._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

object ErrorsAsStringsReadSuite {
  val dummyTimestamp = new Timestamp(1613667600000L)
  val epochTimestamp = new Timestamp(0)
  val dummyText = "hello"

  val expectedSchemaInfer = StructType(
    List(
      StructField("double", DoubleType, true),
      StructField("boolean", BooleanType, true),
      StructField("timestamp", TimestampType, true),
      StructField("string", StringType, true)
    )
  )
  val expectedDataErrorsAsStringsInfer: util.List[Row] =
    List(
      Row(1.0, true, dummyTimestamp, dummyText),
      Row(2.0, false, dummyTimestamp, dummyText),
      Row(0.0, false, epochTimestamp, ""),
      Row(0.0, false, epochTimestamp, "")
    ).asJava

  val expectedDataErrorsAsNullInfer: util.List[Row] =
    List(
      Row(1.0, true, dummyTimestamp, dummyText),
      Row(2.0, false, dummyTimestamp, dummyText),
      Row(null, null, null, null),
      Row(null, null, null, null)
    ).asJava

  val expectedSchemaNonInfer = StructType(
    List(
      StructField("double", StringType, true),
      StructField("boolean", StringType, true),
      StructField("timestamp", StringType, true),
      StructField("string", StringType, true)
    )
  )
  val expectedDataErrorsAsStringsNonInfer: util.List[Row] =
    List(
      Row("1", "TRUE", "19\"-\"Feb\"-\"2021", dummyText),
      Row("2", "FALSE", "19\"-\"Feb\"-\"2021", dummyText),
      Row("", "", "", ""),
      Row("", "", "", "")
    ).asJava

  val expectedDataErrorsAsNullNonInfer: util.List[Row] =
    List(
      Row("1", "TRUE", "19\"-\"Feb\"-\"2021", "hello"),
      Row("2", "FALSE", "19\"-\"Feb\"-\"2021", "hello"),
      Row(null, null, null, null),
      Row(null, null, null, null)
    ).asJava

  val excelLocation = "/spreadsheets/with_errors_all_types.xlsx"
}

class ErrorsAsStringsReadSuite extends AnyFunSpec with DataFrameSuiteBase with Matchers {
  import ErrorsAsStringsReadSuite._

  def readFromResources(path: String, setErrorCellsToFallbackValues: Boolean, inferSchema: Boolean): DataFrame = {
    val url = getClass.getResource(path)
    spark.read
        .excel(setErrorCellsToFallbackValues = setErrorCellsToFallbackValues, inferSchema = inferSchema, excerptSize = 3)
        .load(url.getPath)
  }

  describe("spark-excel") {
    it("should read errors in string format when setErrorCellsToFallbackValues=true and inferSchema=true") {
      val df = readFromResources(excelLocation, true, true)
      val expected = spark.createDataFrame(expectedDataErrorsAsStringsInfer, expectedSchemaInfer)
      assertDataFrameEquals(expected, df)
    }

    it("should read errors as null when setErrorCellsToFallbackValues=false and inferSchema=true") {
      val df = readFromResources(excelLocation, false, true)
      val expected = spark.createDataFrame(expectedDataErrorsAsNullInfer, expectedSchemaInfer)
      assertDataFrameEquals(expected, df)
    }

    it("should read errors in string format when setErrorCellsToFallbackValues=true and inferSchema=false") {
      val df = readFromResources(excelLocation, true, false)
      df.printSchema()
      val expected = spark.createDataFrame(expectedDataErrorsAsStringsNonInfer, expectedSchemaNonInfer)
      expected.printSchema()
      assertDataFrameEquals(expected, df)
    }

    it("should read errors in string format when setErrorCellsToFallbackValues=false and inferSchema=false") {
      val df = readFromResources(excelLocation, false, false)
      val expected = spark.createDataFrame(expectedDataErrorsAsNullNonInfer, expectedSchemaNonInfer)
      assertDataFrameEquals(expected, df)
    }
  }
}
