package com.crealytics.spark.excel
import com.crealytics.spark.excel.Utils.MapIncluding
import com.norbitltd.spoiwo.model.HasIndex._
import com.norbitltd.spoiwo.model.{
  CellDataFormat,
  CellRange,
  CellStyle,
  Table,
  TableColumn,
  Cell => WriteCell,
  Row => WriteRow,
  Sheet => WriteSheet
}
import org.apache.poi.ss.SpreadsheetVersion
import org.apache.poi.ss.usermodel.Row.MissingCellPolicy
import org.apache.poi.ss.usermodel.{Cell, Sheet, Workbook}
import org.apache.poi.ss.util.{AreaReference, CellReference}
import org.apache.poi.xssf.usermodel.{XSSFTable, XSSFWorkbook}
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._
import scala.util.Try

trait DataLocator extends Serializable {
  def dateFormat: Option[String]
  def timestampFormat: Option[String]
  val dateFrmt = dateFormat.getOrElse(ExcelOutputWriter.DEFAULT_DATE_FORMAT)
  val timestampFrmt = timestampFormat.getOrElse(ExcelOutputWriter.DEFAULT_TIMESTAMP_FORMAT)
  def readFrom(workbook: Workbook): Iterator[Seq[Cell]]
  def toSheet(header: Option[Seq[String]], data: Iterator[Seq[Any]], existingWorkbook: Workbook): WriteSheet
}

object DataLocator {
  private def parseRangeAddress(address: String): AreaReference =
    Try {
      val cellRef = new CellReference(address)
      new AreaReference(
        cellRef,
        new CellReference(
          cellRef.getSheetName,
          SpreadsheetVersion.EXCEL2007.getLastRowIndex,
          SpreadsheetVersion.EXCEL2007.getLastColumnIndex,
          false,
          false
        ),
        SpreadsheetVersion.EXCEL2007
      )
    }.getOrElse(new AreaReference(address, SpreadsheetVersion.EXCEL2007))

  val TableAddress = """(.*)\[(.*)\]""".r
  val WithDataAddress = MapIncluding(Seq("dataAddress"), optionally = Seq("dateFormat", "timestampFormat"))
  val WithoutDataAddress = MapIncluding(Seq(), optionally = Seq("dateFormat", "timestampFormat"))
  def apply(parameters: Map[String, String]): DataLocator =
    parameters match {
      case WithDataAddress(Seq(TableAddress(_, _)), _) if parameters.contains("maxRowsInMemory") =>
        throw new IllegalArgumentException(
          s"Reading from a table cannot be combined with maxRowsInMemory, parameters are: $parameters"
        )

      case WithDataAddress(Seq(TableAddress(tableName, "#All")), Seq(dateFormat, timestampFormat)) =>
        new TableDataLocator(tableName, dateFormat, timestampFormat)

      case WithDataAddress(Seq(dataAddress), Seq(dateFormat, timestampFormat)) =>
        new CellRangeAddressDataLocator(
          parseRangeAddress(Option(dataAddress).getOrElse("A1")),
          dateFormat,
          timestampFormat
        )
      case WithoutDataAddress(Seq(), Seq(dateFormat, timestampFormat)) =>
        new CellRangeAddressDataLocator(parseRangeAddress("A1"), dateFormat, timestampFormat)
    }
}

trait AreaDataLocator extends DataLocator {
  def columnIndices(workbook: Workbook): Seq[Int]
  def rowIndices(workbook: Workbook): Seq[Int]
  def sheetName(workbook: Workbook): Option[String]

  def findSheet(workBook: Workbook, sheetName: Option[String]): Option[Sheet] =
    sheetName
      .flatMap(
        sn =>
          Try(Option(workBook.getSheetAt(sn.toInt))).toOption.flatten
            .orElse(Option(workBook.getSheet(sn)))
      )
      .orElse(Try(workBook.getSheetAt(0)).toOption)

  def readFromSheet(workbook: Workbook, name: Option[String]): Iterator[Seq[Cell]] =
    workbook
      .findSheet(name)
      .get
      .iterator
      .asScala
      .filter(r => rowIndices(workbook).contains(r.getRowNum))
      .map(
        r =>
          columnIndices(workbook).filter(_ < r.getLastCellNum).map(r.getCell(_, MissingCellPolicy.CREATE_NULL_AS_BLANK))
      )

  override def toSheet(
    header: Option[Seq[String]],
    data: Iterator[Seq[Any]],
    existingWorkbook: Workbook
  ): WriteSheet = {
    val colInd = columnIndices(existingWorkbook)
    val dataRows: List[WriteRow] = (header.iterator ++ data)
      .zip(rowIndices(existingWorkbook).iterator)
      .map {
        case (row, rowIdx) =>
          WriteRow(
            row.zip(colInd).map {
              case (c, colIdx) => toCell(c, dateFrmt, timestampFrmt).withIndex(colIdx)
            },
            index = rowIdx
          )
      }
      .toList
    sheetName(existingWorkbook).foldLeft(WriteSheet(rows = dataRows))(_ withSheetName _)
  }

  def dateCell(time: Long, format: String): WriteCell = {
    WriteCell(new java.util.Date(time), style = CellStyle(dataFormat = CellDataFormat(format)))
  }
  def toCell(a: Any, dateFormat: String, timestampFormat: String): WriteCell =
    a match {
      case t: java.sql.Timestamp => dateCell(t.getTime, timestampFormat)
      case d: java.sql.Date => dateCell(d.getTime, dateFormat)
      case s: UTF8String => WriteCell(s.toString)
      case s: String => WriteCell(s)
      case f: Float => WriteCell(f.toDouble)
      case d: Double => WriteCell(d)
      case b: Boolean => WriteCell(b)
      case b: Byte => WriteCell(b.toInt)
      case s: Short => WriteCell(s.toInt)
      case i: Int => WriteCell(i)
      case l: Long => WriteCell(l)
      case d: Decimal => WriteCell(d.toBigDecimal)
      case b: BigDecimal => WriteCell(b)
      case b: java.math.BigDecimal => WriteCell(BigDecimal(b))
      case null => WriteCell.Empty
      case e => println(s"Don't know ${e.getClass}"); WriteCell(e.toString)
    }
}

class CellRangeAddressDataLocator(
  val dataAddress: AreaReference,
  val dateFormat: Option[String] = None,
  val timestampFormat: Option[String] = None
) extends AreaDataLocator {
  private val sheetName = Option(dataAddress.getFirstCell.getSheetName)

  def columnIndices(workbook: Workbook): Seq[Int] = (dataAddress.getFirstCell.getCol to dataAddress.getLastCell.getCol)
  def rowIndices(workbook: Workbook): Seq[Int] = (dataAddress.getFirstCell.getRow to dataAddress.getLastCell.getRow)

  override def readFrom(workbook: Workbook): Iterator[Seq[Cell]] = readFromSheet(workbook, sheetName)
  override def sheetName(workbook: Workbook): Option[String] = sheetName
}

class TableDataLocator(
  tableName: String,
  val dateFormat: Option[String] = None,
  val timestampFormat: Option[String] = None
) extends AreaDataLocator {
  override def readFrom(workbook: Workbook): Iterator[Seq[Cell]] = {
    val xwb = workbook.asInstanceOf[XSSFWorkbook]
    readFromSheet(workbook, Some(xwb.getTable(tableName).getSheetName))
  }
  override def toSheet(
    header: Option[Seq[String]],
    data: Iterator[Seq[Any]],
    existingWorkbook: Workbook
  ): WriteSheet = {
    val sheet = super.toSheet(header, data, existingWorkbook)
    val maxRow = sheet.rows.maxIndex
    val minRow = sheet.rows.flatMap(_.index).sorted.headOption.getOrElse(0)
    val maxCol = sheet.rows.map(_.cells.maxIndex).sorted.lastOption.getOrElse(0)
    val minCol = sheet.rows.flatMap(_.cells.flatMap(_.index)).sorted.headOption.getOrElse(0)
    val table =
      Table(cellRange = CellRange(rowRange = (minRow, maxRow), columnRange = (minCol, maxCol)), name = tableName)
    val tableWithPotentialHeader =
      header.foldLeft(table)(
        (tbl, hdr) => tbl.withColumns(hdr.zipWithIndex.map { case (h, i) => TableColumn(h, i) }.toList)
      )
    sheet.withTables(tableWithPotentialHeader)
  }
  def columnIndices(workbook: Workbook): Seq[Int] =
    findTable(workbook).map(t => t.getStartColIndex to t.getEndColIndex).getOrElse(0 until Int.MaxValue)
  override def rowIndices(workbook: Workbook): Seq[Int] =
    findTable(workbook).map(t => t.getStartRowIndex to t.getEndRowIndex).getOrElse(0 until Int.MaxValue)
  override def sheetName(workbook: Workbook): Option[String] =
    findTable(workbook).map(_.getSheetName).orElse(Some(tableName))

  private def findTable(workbook: Workbook): Option[XSSFTable] = {
    Option(workbook.asInstanceOf[XSSFWorkbook].getTable(tableName))
  }
}
