/*
 * Copyright 2022 Martin Mauch (@nightscape)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.crealytics.spark.excel
import org.apache.poi.ss.usermodel.{Cell, CellType, DataFormatter, DateUtil}
import org.apache.spark.sql.types._

import java.math.BigDecimal
import java.sql.{Date, Timestamp}
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
  usePlainNumberFormat: Boolean,
  parseTimestamp: String => Timestamp,
  parseDate: String => Date,
  setErrorCellsToFallbackValues: Boolean
) extends DataColumn {
  def name: String = field.name
  def extractValue(cell: Cell): Any = {
    val cellType = if (cell.getCellType == CellType.FORMULA) cell.getCachedFormulaResultType else cell.getCellType
    if (cellType == CellType.BLANK) {
      return null
    }

    lazy val dataFormatter = new DataFormatter()
    if (usePlainNumberFormat) {
      // Overwrite ExcelGeneralNumberFormat with custom PlainNumberFormat.
      // See https://github.com/crealytics/spark-excel/issues/321
      lazy val plainNumberFormat = PlainNumberFormat
      dataFormatter.addFormat("General", plainNumberFormat)
      dataFormatter.addFormat("@", plainNumberFormat)
    }

    if (cellType == CellType.ERROR && !setErrorCellsToFallbackValues) {
      return null
    }

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
            case CellType.ERROR => Some(0.0)
            case _ => None
          }
        case _ => throw new RuntimeException(s"Unsupported numeric value extraction from $cell")
      }
    lazy val booleanValue = cell.getCellType match {
      case CellType.BOOLEAN => Option(cell.getBooleanCellValue)
      case CellType.STRING => Option(cell.getStringCellValue).map(_.toBoolean)
      case _ => throw new RuntimeException(s"Unsupported boolean value extraction from $cell")
    }
    lazy val bigDecimal = numericValue.map(new BigDecimal(_))
    val value: Option[Any] = field.dataType match {
      case _: ByteType => if (cellType == CellType.ERROR) Some(0) else numericValue.map(_.toByte)
      case _: ShortType => if (cellType == CellType.ERROR) Some(0) else numericValue.map(_.toShort)
      case _: IntegerType => if (cellType == CellType.ERROR) Some(0) else numericValue.map(_.toInt)
      case _: LongType => if (cellType == CellType.ERROR) Some(0.0) else numericValue.map(_.toLong)
      case _: FloatType => if (cellType == CellType.ERROR) Some(0.0) else numericValue.map(_.toFloat)
      case _: DoubleType => if (cellType == CellType.ERROR) Some(0.0) else numericValue
      case _: BooleanType => if (cellType == CellType.ERROR) Some(false) else booleanValue
      case _: DecimalType =>
        if (cellType == CellType.ERROR) Some(0.0)
        else if (cellType == CellType.STRING && cell.getStringCellValue == "") None
        else bigDecimal
      case _: TimestampType =>
        cellType match {
          case CellType.ERROR =>
            Some(new Timestamp(0))
          case CellType.NUMERIC | CellType.FORMULA =>
            numericValue.map(n => new Timestamp(DateUtil.getJavaDate(n).getTime))
          case _ => stringValue.filter(_.trim.nonEmpty).map(parseTimestamp)
        }
      case _: DateType =>
        cellType match {
          case CellType.ERROR => Some(new java.sql.Date(0))
          case CellType.NUMERIC | CellType.FORMULA =>
            numericValue.map(n => new Date(DateUtil.getJavaDate(n).getTime))
          case _ => stringValue.filter(_.trim.nonEmpty).map(parseDate)
        }

      case _: StringType =>
        if (cellType == CellType.ERROR) Some("")
        else stringValue.filterNot(_.isEmpty && treatEmptyValuesAsNulls)
      case t => throw new RuntimeException(s"Unsupported cast from $cell to $t")
    }

    value.orNull
  }

  private def stringToDouble(value: String): Double = {
    Try(value.toDouble) match {
      case Success(d) => d
      case Failure(_) => Double.NaN
    }
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
