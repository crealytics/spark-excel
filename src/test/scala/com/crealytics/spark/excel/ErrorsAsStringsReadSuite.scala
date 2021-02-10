package com.crealytics.spark.excel

import java.util

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.{Row, _}
import org.apache.spark.sql.types._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

object ErrorsAsStringsReadSuite {
  val expectedSchemaErrorsAsStringsInfer = StructType(
    List(StructField("id", DoubleType, true), StructField("errors", StringType, true))
  )

  val expectedDataErrorsAsStringsInfer: util.List[Row] =
    List(Row(1.0, "#N/A"), Row(2.0, "#NULL!")).asJava

  val expectedSchemaErrorsAsNullInfer = StructType(
    List(StructField("id", DoubleType, true), StructField("errors", StringType, true))
  )

  val expectedDataErrorsAsNullInfer: util.List[Row] =
    List(Row(1.0, null), Row(2.0, null)).asJava

  val expectedSchemaNonInfer = StructType(
    List(StructField("id", StringType, true), StructField("errors", StringType, true))
  )

  val expectedDataErrorsNonInfer: util.List[Row] =
    List(Row("1", "#N/A"), Row("2", "#NULL!")).asJava

  val excelLocation = "/spreadsheets/with_errors.xlsx"
}

class ErrorsAsStringsReadSuite extends AnyFunSpec with DataFrameSuiteBase with Matchers {
  import ErrorsAsStringsReadSuite._

  def readFromResources(path: String, treatErrorsAsStrings: Boolean, inferSchema: Boolean): DataFrame = {
    val url = getClass.getResource(path)
    spark.read.excel(treatErrorsAsStrings = treatErrorsAsStrings, inferSchema = inferSchema).load(url.getPath)
  }

  describe("spark-excel") {
    it("should read errors in string format when treatErrorsAsStrings=true and inferSchema=true") {
      val df = readFromResources(excelLocation, true, true)
      val expected = spark.createDataFrame(expectedDataErrorsAsStringsInfer, expectedSchemaErrorsAsStringsInfer)
      assertDataFrameEquals(expected, df)
    }

    // We cannot set individual ERROR cells to null as the final inferred column type will always be StringType.
    //    it("should read errors as null when treatErrorsAsStrings=false and inferSchema=true") {
    //      val df = readFromResources(excelLocation, false, true)
    //
    //      // scalastyle:offprintln
    //      println("actual")
    //      // scalastyle:onprintln
    //      df.printSchema()
    //      df.show()
    //
    //      val expected = spark.createDataFrame(expectedDataErrorsAsNullInfer, expectedSchemaErrorsAsNullInfer)
    //
    //      // scalastyle:offprintln
    //      println("expected")
    //      // scalastyle:onprintln
    //      expected.printSchema()
    //      expected.show()
    //
    //      assertDataFrameEquals(expected, df)
    //    }

    it("should read errors in string format when treatErrorsAsStrings=true and inferSchema=false") {
      val df = readFromResources(excelLocation, true, false)
      val expected = spark.createDataFrame(expectedDataErrorsNonInfer, expectedSchemaNonInfer)
      assertDataFrameEquals(expected, df)
    }

    it("should read errors in string format when treatErrorsAsStrings=false and inferSchema=false") {
      val df = readFromResources(excelLocation, false, false)
      val expected = spark.createDataFrame(expectedDataErrorsNonInfer, expectedSchemaNonInfer)
      assertDataFrameEquals(expected, df)
    }
  }
}
