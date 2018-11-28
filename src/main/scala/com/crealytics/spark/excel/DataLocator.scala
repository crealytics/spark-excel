package com.crealytics.spark.excel
import com.crealytics.spark.excel.Utils.MapIncluding
import com.norbitltd.spoiwo.model.HasIndex._
import com.norbitltd.spoiwo.model.{
  CellDataFormat,
  CellRange,
  CellStyle,
  HasIndex,
  Table,
  Cell => WriteCell,
  Row => WriteRow,
  Sheet => WriteSheet
}
import org.apache.poi.ss.usermodel.{Cell, Row, Sheet, Workbook}
import org.apache.poi.ss.util.{CellRangeAddress, CellReference}
import org.apache.poi.xssf.usermodel.{XSSFTable, XSSFWorkbook}

import scala.collection.JavaConverters._
import scala.util.Try

trait DataLocator {
  def readFrom(workbook: Workbook): Iterator[Seq[Cell]]
  def toSheet(
    header: Option[Seq[String]],
    data: Iterator[Seq[Any]],
    dateFormat: String,
    timestampFormat: String,
    existingWorkbook: Workbook
  ): WriteSheet
}

object DataLocator {

  private def parseRangeAddress(address: String): CellRangeAddress =
    Try {
      val cellRef = new CellReference(address)
      new CellRangeAddress(cellRef.getRow, Int.MaxValue, cellRef.getCol, Int.MaxValue)
    }.getOrElse(CellRangeAddress.valueOf(address))

  val WithTableName = MapIncluding(Seq("tableName"))
  val WithSheetAndAddress = MapIncluding(Seq(), optionally = Seq("sheetName", "dataAddress"))
  def apply(parameters: Map[String, String]): DataLocator = parameters match {
    case WithTableName(Seq(tableName)) if parameters.contains("maxRowsInMemory") =>
      throw new IllegalArgumentException(s"tableName option cannot be combined with maxRowsInMemory")
    case WithTableName(Seq(tableName)) => new TableDataLocator(tableName.toString)
    case WithSheetAndAddress(Seq(), Seq(sheetName, dataAddress)) =>
      new CellRangeAddressDataLocator(sheetName, parseRangeAddress(dataAddress.getOrElse("A1")))
  }
}

trait AreaDataLocator extends DataLocator {
  def startColumn(workbook: Workbook): Int
  def endColumn(workbook: Workbook): Int
  def startRow(workbook: Workbook): Int
  def endRow(workbook: Workbook): Int
  def sheetName(workbook: Workbook): Option[String]

  implicit class RichRowIterator(iter: Iterator[Row]) {
    def withinStartAndEndRow(startRow: Int, endRow: Int): Iterator[Row] =
      iter.dropWhile(_.getRowNum < startRow).takeWhile(_.getRowNum <= endRow)
  }

  def findSheet(workBook: Workbook, sheetName: Option[String]): Sheet =
    sheetName
      .map(
        sn =>
          Try(Option(workBook.getSheetAt(sn.toInt))).toOption.flatten
            .orElse(Option(workBook.getSheet(sn)))
            .getOrElse(throw new IllegalArgumentException(s"Unknown sheet $sn"))
      )
      .getOrElse(workBook.getSheetAt(0))

  def readFromSheet(workbook: Workbook, name: Option[String]): Iterator[Vector[Cell]] = {
    val sheet = findSheet(workbook, name)
    val rowStart = startRow(workbook)
    val rowEnd = endRow(workbook)
    val colStart = startColumn(workbook)
    val colEnd = endColumn(workbook)
    sheet.iterator.asScala
      .withinStartAndEndRow(rowStart, rowEnd)
      .map(_.cellIterator().asScala.filter(c => c.getColumnIndex >= colStart && c.getColumnIndex <= colEnd).to[Vector])
  }

  override def toSheet(
    header: Option[Seq[String]],
    data: Iterator[Seq[Any]],
    dateFormat: String,
    timestampFormat: String,
    existingWorkbook: Workbook
  ): WriteSheet = {
    val dataRows: List[WriteRow] = (header.iterator ++ data).zipWithIndex.map {
      case (row, idx) =>
        WriteRow(row.zipWithIndex.map {
          case (c, idx) => toCell(c, dateFormat, timestampFormat).withIndex(idx + startColumn(existingWorkbook))
        }, index = idx + startRow(existingWorkbook))
    }.toList
    sheetName(existingWorkbook).foldLeft(WriteSheet(rows = dataRows))(_ withSheetName _)
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
}

class CellRangeAddressDataLocator(sheetName: Option[String], dataAddress: CellRangeAddress) extends AreaDataLocator {

  def startColumn(workbook: Workbook): Int = dataAddress.getFirstColumn
  def endColumn(workbook: Workbook): Int = dataAddress.getLastColumn
  def startRow(workbook: Workbook): Int = dataAddress.getFirstRow
  def endRow(workbook: Workbook): Int = dataAddress.getLastRow

  override def readFrom(workbook: Workbook): Iterator[Seq[Cell]] = readFromSheet(workbook, sheetName)
  override def sheetName(workbook: Workbook): Option[String] = sheetName
}

class TableDataLocator(tableName: String) extends AreaDataLocator {
  override def readFrom(workbook: Workbook): Iterator[Seq[Cell]] = {
    val xwb = workbook.asInstanceOf[XSSFWorkbook]
    readFromSheet(workbook, Some(xwb.getTable(tableName).getSheetName))
  }
  override def toSheet(
    header: Option[Seq[String]],
    data: Iterator[Seq[Any]],
    dateFormat: String,
    timestampFormat: String,
    existingWorkbook: Workbook
  ): WriteSheet = {
    val sheet = super.toSheet(header, data, dateFormat, timestampFormat, existingWorkbook)
    val maxRow = sheet.rows.maxIndex
    val minRow = sheet.rows.flatMap(_.index).sorted.headOption.getOrElse(0)
    val maxCol = sheet.rows.map(_.cells.maxIndex).sorted.lastOption.getOrElse(0)
    val minCol = sheet.rows.flatMap(_.cells.flatMap(_.index)).sorted.headOption.getOrElse(0)
    sheet.withTables(
      Table(cellRange = CellRange(rowRange = (minRow, maxRow), columnRange = (minCol, maxCol)), name = tableName)
    )
  }
  override def startColumn(workbook: Workbook): Int = findTable(workbook).map(_.getStartColIndex).getOrElse(0)
  override def endColumn(workbook: Workbook): Int = findTable(workbook).map(_.getEndColIndex).getOrElse(Int.MaxValue)
  override def startRow(workbook: Workbook): Int = findTable(workbook).map(_.getStartRowIndex).getOrElse(0)
  override def endRow(workbook: Workbook): Int = findTable(workbook).map(_.getEndRowIndex).getOrElse(Int.MaxValue)
  override def sheetName(workbook: Workbook): Option[String] =
    findTable(workbook).map(_.getSheetName).orElse(Some(tableName))

  private def findTable(workbook: Workbook): Option[XSSFTable] = {
    Option(workbook.asInstanceOf[XSSFWorkbook].getTable(tableName))
  }
}
