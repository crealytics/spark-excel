package com.crealytics.spark.excel
import com.norbitltd.spoiwo.model.{CellDataFormat, CellStyle, Cell => WriteCell, Row => WriteRow, Sheet => WriteSheet}
import org.apache.poi.ss.usermodel.{Cell, Row, Sheet, Workbook}
import org.apache.poi.ss.util.{CellRangeAddress, CellReference}

import scala.collection.JavaConverters._
import scala.util.Try

trait DataLocator {
  def readFrom(workbook: Workbook): Iterator[Seq[Cell]]
  def toSheet(
    header: Option[Seq[String]],
    data: Iterator[Seq[Any]],
    dateFormat: String,
    timestampFormat: String
  ): WriteSheet
}

object DataLocator {

  private def parseRangeAddress(address: String): CellRangeAddress =
    Try {
      val cellRef = new CellReference(address)
      new CellRangeAddress(cellRef.getRow, Int.MaxValue, cellRef.getCol, Int.MaxValue)
    }.getOrElse(CellRangeAddress.valueOf(address))

  def apply(parameters: Map[String, String]): DataLocator = {
    new CellRangeAddressDataLocator(
      parameters.get("sheetName"),
      parseRangeAddress(parameters.getOrElse("dataAddress", "A1"))
    )
  }
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
  def dateCell(time: Long, format: String): WriteCell = {
    WriteCell(new java.util.Date(time), style = CellStyle(dataFormat = CellDataFormat(format)))
  }
  def toCell(a: Any, dateFormat: String, timestampFormat: String): WriteCell = a match {
    case t: java.sql.Timestamp => dateCell(t.getTime, timestampFormat)
    case d: java.sql.Date => dateCell(d.getTime, dateFormat)
    case s: String => WriteCell(s)
    case f: Float => WriteCell(f.toDouble)
    case d: Double => WriteCell(d)
    case b: Boolean => WriteCell(b)
    case b: Byte => WriteCell(b.toInt)
    case s: Short => WriteCell(s.toInt)
    case i: Int => WriteCell(i)
    case l: Long => WriteCell(l)
    case b: BigDecimal => WriteCell(b)
    case b: java.math.BigDecimal => WriteCell(BigDecimal(b))
    case null => WriteCell.Empty
  }
  override def toSheet(
    header: Option[Seq[String]],
    data: Iterator[Seq[Any]],
    dateFormat: String,
    timestampFormat: String
  ): WriteSheet = {
    val dataRows: List[WriteRow] = (header.iterator ++ data).zipWithIndex.map {
      case (row, idx) =>
        WriteRow(row.zipWithIndex.map {
          case (c, idx) => toCell(c, dateFormat, timestampFormat).withIndex(idx + startColumn)
        }, index = idx + startRow)
    }.toList
    sheetName.foldLeft(WriteSheet(rows = dataRows))(_ withSheetName _)
  }
}
