package com.crealytics.spark.excel

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.scalatest.FunSpec
import org.scalatest.Matchers
import scala.collection.JavaConverters._
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import java.io.File
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import java.io.FileOutputStream
import org.scalatest.prop.PropertyChecks
import org.scalacheck.{Arbitrary, Gen, Shrink}

class ReadDefaultSheetSuite extends FunSpec with PropertyChecks with DataFrameSuiteBase with Matchers {
  import ReadDefaultSheetSuite._

  describe("with no sheet name specified") {
    it(s"should read a first sheet from xls file") {
      shouldReadSheet(streaming = false, xslx = false)
    }
    it(s"should read a first sheet from xlsx file") {
      shouldReadSheet(streaming = false, xslx = true)
    }
    it(s"should read a first sheet from xlsx file using streaming api") {
      shouldReadSheet(streaming = true, xslx = true)
    }

  }

  def shouldReadSheet(streaming: Boolean, xslx: Boolean): Unit = {
    forAll(Gen.choose(1, 10), MinSuccessful(3)) { numSheets =>
      val spreadsheet = generateSpreadSheetCached(numSheets, xslx = xslx)
      val df =
        spark.read
          .excel(useHeader = true)
          .schema(simpleSchema)
          .withStreaming(Some(10).filter(_ => streaming))
          .load(spreadsheet)
      assertDataFrameEquals(expectedData(sheetIndex = 1)(spark), df)
    }
  }

}

object ReadDefaultSheetSuite {

  var spreadSheetCache = Map.empty[(Int, Boolean), String]
  def generateSpreadSheetCached(numSheets: Int, xslx: Boolean) = this.synchronized {
    def genNewValue = {
      val path = generateSpreadSheet(numSheets, xslx)
      spreadSheetCache += (((numSheets, xslx), path))
      path
    }
    spreadSheetCache.get((numSheets, xslx)).fold(genNewValue)(identity)
  }

  def generateSpreadSheet(numSheets: Int, xslx: Boolean): String = {
    val ext = if (xslx) ".xslx" else ".xsl"
    val filename = File.createTempFile("spark_excel_test_default_sheet_read_", ext).getAbsolutePath

    val wb = if (xslx) new XSSFWorkbook() else new HSSFWorkbook()

    (1 to numSheets).foreach { sheetIndex =>
      val sheet = wb.createSheet(s"sheet-$sheetIndex")
      val headerRow = sheet.createRow(0)
      headerRow.createCell(0).setCellValue("A")
      headerRow.createCell(1).setCellValue("B")
      headerRow.createCell(2).setCellValue("C")

      for {
        r <- 1 to 5
        row = sheet.createRow(r)
        _ = row.createCell(0).setCellValue(s"cell-from-$sheetIndex")
        c <- 1 to 2
      } {
        row.createCell(c).setCellValue(c)
      }

    }

    val os = new FileOutputStream(filename)
    wb.write(os)
    wb.close()
    os.close()

    filename
  }

  implicit class ReachDataframeReader(reader: DataFrameReader) {
    def withSchema(schema: Option[StructType]) = schema.foldLeft(reader.option("inferSchema", true))(_ schema _)
    def withStreaming(maxRowsInMemory: Option[Int]) = maxRowsInMemory.foldLeft(reader)(_.option("maxRowsInMemory", _))
  }

  def expectedData(sheetIndex: Int)(implicit spark: SparkSession): DataFrame = {
    val rows = (for (r <- 1 to 5) yield {
      Row(s"cell-from-$sheetIndex", 1.0d, 2.0d)
    })
    spark.createDataFrame(rows.asJava, simpleSchema)
  }

  val simpleSchema = StructType(
    List(StructField("A", StringType, true), StructField("B", DoubleType, true), StructField("C", DoubleType, true))
  )

}
