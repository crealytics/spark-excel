package com.crealytics.spark.excel

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import scala.collection.JavaConverters._

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

object EncryptedReadSuite {
  val simpleSchema = StructType(
    List(
      StructField("A", DoubleType, true),
      StructField("B", DoubleType, true),
      StructField("C", DoubleType, true),
      StructField("D", DoubleType, true)
    )
  )

  val expectedData = List(Row(1.0d, 2.0d, 3.0d, 4.0d)).asJava
}

class EncryptedReadSuite extends AnyFunSpec with DataFrameSuiteBase with Matchers {
  import EncryptedReadSuite._

  lazy val expected = spark.createDataFrame(expectedData, simpleSchema)

  def readFromResources(path: String, password: String, maxRowsInMemory: Option[Int] = None): DataFrame = {
    val url = getClass.getResource(path)
    val reader = spark.read
      .excel(
        dataAddress = s"Sheet1!A1",
        treatEmptyValuesAsNulls = true,
        workbookPassword = password,
        inferSchema = true
      )
    val withMaxRows = maxRowsInMemory.fold(reader)(rows => reader.option("maxRowsInMemory", s"$rows"))
    withMaxRows.load(url.getPath)
  }

  describe("spark-excel") {
    it("should read encrypted xslx file") {
      val df = readFromResources("/spreadsheets/simple_encrypted.xlsx", "fooba")

      assertDataFrameEquals(expected, df)
    }

    it("should read encrypted xlsx file with maxRowsInMem=10") {
      val df = readFromResources("/spreadsheets/simple_encrypted.xlsx", "fooba", maxRowsInMemory = Some(10))

      assertDataFrameEquals(expected, df)
    }

    it("should read encrypted xlsx file with maxRowsInMem=1") {
      val df = readFromResources("/spreadsheets/simple_encrypted.xlsx", "fooba", maxRowsInMemory = Some(1))

      assertDataFrameEquals(expected, df)
    }

    it("should read encrypted xls file") {
      val df = readFromResources("/spreadsheets/simple_encrypted.xls", "fooba")

      assertDataFrameEquals(expected, df)
    }
  }
}
