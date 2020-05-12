package com.crealytics.spark.excel
import java.math.BigDecimal
import java.sql.Timestamp

import org.apache.poi.ss.usermodel.{Cell, CellType, DataFormatter, DateUtil}
import org.apache.spark.sql.types._

import scala.util.{Failure, Success, Try}

trait DataColumn extends PartialFunction[Seq[Cell], Any] {
  def name: String
  def columnIndex: Int
  def findCell(cells: Seq[Cell]): Option[Cell] = cells.find(_.getColumnIndex == columnIndex)
  def extractValue(cell: Cell): Any
  override def isDefinedAt(cells: scala.Seq[Cell]): Boolean = findCell(cells).isDefined
  def apply(cells: Seq[Cell]): Any = extractValue(findCell(cells).get)
}

class HeaderDataColumn(
  val field: StructField,
  val columnIndex: Int,
  treatEmptyValuesAsNulls: Boolean,
  parseTimestamp: String => Timestamp
) extends DataColumn {
  def name: String = field.name
  def extractValue(cell: Cell): Any = {
    val cellType = cell.getCellType
    if (cellType == CellType.BLANK) {
      return null
    }

    val value: Option[Any] = field.dataType match {
      case _: ByteType => cell.numericValue.map(_.toByte)
      case _: ShortType => cell.numericValue.map(_.toShort)
      case _: IntegerType => cell.numericValue.map(_.toInt)
      case _: LongType => cell.numericValue.map(_.toLong)
      case _: FloatType => cell.numericValue.map(_.toFloat)
      case _: DoubleType => cell.numericValue
      case _: BooleanType => cell.booleanValue
      case _: DecimalType =>
        if (cellType == CellType.STRING && cell.getStringCellValue == "") None else cell.bigDecimalValue
      case _: TimestampType =>
        cellType match {
          case CellType.NUMERIC | CellType.FORMULA =>
            cell.numericValue.map(n => new Timestamp(DateUtil.getJavaDate(n).getTime))
          case _ => cell.stringValue.filter(_.trim.nonEmpty).map(parseTimestamp)
        }
      case _: DateType => cell.numericValue.map(n => new java.sql.Date(DateUtil.getJavaDate(n).getTime))
      case _: StringType =>
        cell.stringValue.filterNot(_.isEmpty && treatEmptyValuesAsNulls)
      case t => throw new RuntimeException(s"Unsupported cast from $cell to $t")
    }

    value.orNull
  }
}

class ColorDataColumn(val name: String, val columnIndex: Int) extends DataColumn {
  def extractValue(cell: Cell): String =
    cell.getCellStyle.getFillForegroundColorColor match {
      case null => ""
      case c: org.apache.poi.xssf.usermodel.XSSFColor => c.getARGBHex
      case c => throw new RuntimeException(s"Unknown color type $c: ${c.getClass}")
    }
}
