package com.crealytics.spark.excel

import com.norbitltd.spoiwo.model.Workbook
import com.norbitltd.spoiwo.natures.xlsx.Model2XlsxConversions._
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.scalacheck.Gen
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.JavaConverters._

class DataLocatorSuite extends AnyFunSpec with ScalaCheckPropertyChecks with Matchers with Generators {
  describe("with a table reference") {
    val dl = DataLocator(Map("dataAddress" -> s"$tableName[#All]"))
    describe("containing #All") {
      it("extracts the entire table data") {
        forAll(sheetWithTableGen) { sheet =>
          val actualData = dl.readFrom(sheet.convertAsXlsx()).map(_.map(_.value)).to[Seq]
          actualData should contain theSameElementsAs sheet.extractTableData(0)
        }
      }

      it("writes into a new table in a new sheet if no corresponding table exists") {
        forAll(sheetGenerator(withHeader = Gen.const(true), numCols = Gen.choose(1, 200))) { dataSheet =>
          val workbook = new XSSFWorkbook()
          val header = dataSheet.rows.head.cells.map(_.value.toString).toSeq
          val generatedSheet = dl.toSheet(
            header = Some(header),
            data = dataSheet.rows.tail.iterator.map(_.cells.map(_.value.toString).toSeq),
            existingWorkbook = workbook
          )
          generatedSheet.convertAsXlsx(workbook)
          val pTable = workbook.getTable(tableName)
          pTable.getSheetName should equal(tableName)
          pTable.getColumns.asScala.map(_.getName) should contain theSameElementsInOrderAs header
          val actualData = dl.readFrom(workbook).map(_.map(_.value)).to[Seq]
          actualData should contain theSameElementsAs dataSheet.rows.map(_.cells.map(_.value))
        }
      }

      it("overwrites an existing table") {
        forAll(sheetWithTableGen) { sheetWithTable =>
          val workbook = sheetWithTable.convertAsXlsx()
          val table = sheetWithTable.tables.head
          val header = table.columns.map(_.name)
          val tableData = dl.readFrom(workbook).map(_.map(c => s"new_$c")).toList
          val generatedSheet =
            dl.toSheet(header = tableData.headOption, data = tableData.iterator.drop(1), existingWorkbook = workbook)
          Workbook(generatedSheet).writeToExisting(workbook)
          val pTable = workbook.getTable(tableName)
          pTable.getSheetName should equal(sheetName)
          pTable.getColumns.asScala.map(_.getName) should contain theSameElementsInOrderAs header
          val actualData = dl.readFrom(workbook).map(_.map(_.value)).to[Seq]
          actualData should contain theSameElementsAs tableData
        }
      }
    }
  }
  describe("without any dataAddress") {
    it("defaults to starting at cell A1 in the first sheet") {
      val dl = DataLocator(Map())
      dl shouldBe a[CellRangeAddressDataLocator]
      val cradl = dl.asInstanceOf[CellRangeAddressDataLocator]
      cradl.dataAddress.getFirstCell.formatAsString() should equal("A1")
      cradl.dataAddress.getFirstCell.getSheetName should equal(null)
    }
  }
}
