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
  val name: String,
  val columnIndex: Int,
  dataType: DataType,
  treatEmptyValuesAsNulls: Boolean,
  parseTimestamp: String => Timestamp
) extends DataColumn {
  def extractValue(cell: Cell): Any = {
    val cellType = cell.getCellType
    if (cellType == CellType.BLANK) {
      return null
    }

    lazy val dataFormatter = new DataFormatter()
    lazy val stringValue =
      cell.getCellType match {
        case CellType.FORMULA =>
          cell.getCachedFormulaResultType match {
            case CellType.STRING => Option(cell.getRichStringCellValue).map(_.getString)
            case CellType.NUMERIC => Option(cell.getNumericCellValue).map(_.toString)
            case CellType.BLANK => None
            case _ => Some(dataFormatter.formatCellValue(cell))
          }
        case CellType.BLANK => None
        case _ => Some(dataFormatter.formatCellValue(cell))
      }
    def parseNumber(string: Option[String]): Option[Double] = string.filter(_.trim.nonEmpty).map(stringToDouble)
    lazy val numericValue =
      cell.getCellType match {
        case CellType.NUMERIC => Option(cell.getNumericCellValue)
        case CellType.STRING => parseNumber(Option(cell.getStringCellValue))
        case CellType.FORMULA =>
          cell.getCachedFormulaResultType match {
            case CellType.NUMERIC => Option(cell.getNumericCellValue)
            case CellType.STRING =>
              parseNumber(Option(cell.getRichStringCellValue).map(_.getString))
          }
      }
    lazy val bigDecimal = numericValue.map(new BigDecimal(_))
    val value: Option[Any] = dataType match {
      case _: ByteType => numericValue.map(_.toByte)
      case _: ShortType => numericValue.map(_.toShort)
      case _: IntegerType => numericValue.map(_.toInt)
      case _: LongType => numericValue.map(_.toLong)
      case _: FloatType => numericValue.map(_.toFloat)
      case _: DoubleType => numericValue
      case _: BooleanType => Option(cell.getBooleanCellValue)
      case _: DecimalType =>
        if (cellType == CellType.STRING && cell.getStringCellValue == "") None else bigDecimal
      case _: TimestampType =>
        cellType match {
          case CellType.NUMERIC => numericValue.map(n => new Timestamp(DateUtil.getJavaDate(n).getTime))
          case _ => stringValue.filter(_.trim.nonEmpty).map(parseTimestamp)
        }
      case _: DateType => numericValue.map(n => new java.sql.Date(DateUtil.getJavaDate(n).getTime))
      case _: StringType =>
        stringValue.filterNot(_.isEmpty && treatEmptyValuesAsNulls)
      case t => throw new RuntimeException(s"Unsupported cast from $cell to $t")
    }
    value.orElse(null)
  }

  private def stringToDouble(value: String): Double = {
    Try(value.toDouble) match {
      case Success(d) => d
      case Failure(_) => Double.NaN
    }
  }
}

class ColorDataColumn(val name: String, val columnIndex: Int) extends DataColumn {
  def extractValue(cell: Cell): String = cell.getCellStyle.getFillForegroundColorColor match {
    case null => ""
    case c: org.apache.poi.xssf.usermodel.XSSFColor => c.getARGBHex
    case c => throw new RuntimeException(s"Unknown color type $c: ${c.getClass}")
  }
}
