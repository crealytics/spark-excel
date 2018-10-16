package com.crealytics.spark.excel
import org.apache.poi.ss.SpreadsheetVersion
import org.apache.poi.ss.util.AreaReference
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import com.norbitltd.spoiwo.natures.xlsx.Model2XlsxConversions._
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.prop.PropertyChecks

class DataLocatorSuite extends FunSpec with PropertyChecks with Matchers with Generators {
  describe("when parsing data addresses") {
    describe("with a table reference") {
      describe("containing #All") {
        it("parses correctly") {
          forAll(sheetWithTableGen) { sheet =>
            val workbook = sheet.convertAsXlsx()
            val tableName = sheet.tables.head.name.get
          // val dl = DataLocator(Map("dataAddress" -> s"$tableName[#All]"))
          // dl shouldBe a[CellRangeAddressDataLocator]
          }
        }
      }
    }
  }
}
