package com.crealytics.spark.excel
import org.apache.poi.ss.usermodel.{Cell, Row, Sheet, Workbook}
import org.apache.poi.ss.util.{CellRangeAddress, CellReference}

import scala.collection.JavaConverters._
import scala.util.Try

trait DataLocator {
  def readFrom(workbook: Workbook): Iterator[Seq[Cell]]
}

class CellRangeAddressDataLocator(sheetName: Option[String], dataAddress: CellRangeAddress) extends DataLocator {

  private val startColumn = dataAddress.getFirstColumn
  private val endColumn = dataAddress.getLastColumn
  private val startRow = dataAddress.getFirstRow
  private val endRow = dataAddress.getLastRow

  implicit class RichRowIterator(iter: Iterator[Row]) {
    def withinStartAndEndRow(startRow: Int, endRow: Int): Iterator[Row] =
      iter.dropWhile(_.getRowNum < startRow).takeWhile(_.getRowNum <= endRow)
  }
  private def findSheet(workBook: Workbook, sheetName: Option[String]): Sheet =
    sheetName
      .map(
        sn =>
          Try(Option(workBook.getSheetAt(sn.toInt))).toOption.flatten
            .orElse(Option(workBook.getSheet(sn)))
            .getOrElse(throw new IllegalArgumentException(s"Unknown sheet $sn"))
      )
      .getOrElse(workBook.getSheetAt(0))

  override def readFrom(workbook: Workbook): Iterator[Seq[Cell]] = {
    val sheet = findSheet(workbook, sheetName)
    sheet.iterator.asScala
      .withinStartAndEndRow(startRow, endRow)
      .map(
        r =>
          r.cellIterator()
            .asScala
            .filter(c => c.getColumnIndex >= startColumn && c.getColumnIndex <= endColumn)
            .to[Vector]
      )
  }
}
