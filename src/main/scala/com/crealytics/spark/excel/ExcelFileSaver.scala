package com.crealytics.spark.excel

import java.io.BufferedOutputStream

import com.crealytics.spark.excel.ExcelFileSaver.{DEFAULT_DATE_FORMAT, DEFAULT_SHEET_NAME, DEFAULT_TIMESTAMP_FORMAT}
import com.norbitltd.spoiwo.model._
import com.norbitltd.spoiwo.natures.xlsx.Model2XlsxConversions._
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.poi.ss.util.CellRangeAddress
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.JavaConverters._

object ExcelFileSaver {
  final val DEFAULT_SHEET_NAME = "Sheet1"
  final val DEFAULT_DATE_FORMAT = "yy-m-d h:mm"
  final val DEFAULT_TIMESTAMP_FORMAT = "yyyy-mm-dd hh:mm:ss.000"
}

class ExcelFileSaver(
  fs: FileSystem,
  location: Path,
  dataFrame: DataFrame,
  saveMode: SaveMode,
  sheetName: String,
  dataAddress: CellRangeAddress,
  useHeader: Boolean = true,
  dateFormat: String = DEFAULT_DATE_FORMAT,
  timestampFormat: String = DEFAULT_TIMESTAMP_FORMAT,
  preHeader: Option[String]
) {
  private def startColumn = dataAddress.getFirstColumn
  private def endColumn = dataAddress.getLastColumn
  private def startRow = dataAddress.getFirstRow

  def save(): Unit = {
    lazy val sheet = {
      val preHeaderRows =
        preHeader.toList.flatMap(_.split("\\R", -1).zipWithIndex.map {
          case (line, idx) =>
            Row(line.split("\t").zipWithIndex.map { case (str, idx) => Cell(str, index = idx) }, index = startRow + idx)
        })
      val preHeaderOffset = startRow + preHeaderRows.size
      val headerRow = Row(dataFrame.schema.fields.zipWithIndex.map {
        case (f, idx) => Cell(f.name, index = idx + startColumn)
      }, index = preHeaderOffset)
      val headerOffset = preHeaderOffset + (if (useHeader) 1 else 0)
      val dataRows = dataFrame
        .toLocalIterator()
        .asScala
        .zipWithIndex
        .map {
          case (row, idx) =>
            Row(row.toSeq.zipWithIndex.map {
              case (c, idx) => toCell(c, dateFormat, timestampFormat).withIndex(idx + startColumn)
            }, index = idx + headerOffset)
        }
        .toList
      val rows = preHeaderRows ++ (if (useHeader) headerRow :: dataRows else dataRows)
      Sheet(name = sheetName, rows = rows)
    }
    val fileAlreadyExists = fs.exists(location)
    (fileAlreadyExists, saveMode) match {
      case (false, _) | (_, SaveMode.Overwrite) =>
        if (fileAlreadyExists) {
          fs.delete(location, true)
        }
        autoClose(new BufferedOutputStream(fs.create(location)))(sheet.convertAsXlsx.write)
      case (true, SaveMode.ErrorIfExists) =>
        sys.error(s"path $location already exists.")
      case (true, SaveMode.Ignore) => ()
      case (true, SaveMode.Append) =>
        val inputStream: FSDataInputStream = fs.open(location)
        val workbook = new XSSFWorkbook(inputStream)
        Workbook(sheet).writeToExisting(workbook)
        autoClose(new BufferedOutputStream(fs.create(location)))(workbook.write)
    }
  }
  def dateCell(time: Long, format: String): Cell = {
    Cell(new java.util.Date(time), style = CellStyle(dataFormat = CellDataFormat(format)))
  }
  def toCell(a: Any, dateFormat: String, timestampFormat: String): Cell = a match {
    case t: java.sql.Timestamp => dateCell(t.getTime, timestampFormat)
    case d: java.sql.Date => dateCell(d.getTime, dateFormat)
    case s: String => Cell(s)
    case f: Float => Cell(f.toDouble)
    case d: Double => Cell(d)
    case b: Boolean => Cell(b)
    case b: Byte => Cell(b.toInt)
    case s: Short => Cell(s.toInt)
    case i: Int => Cell(i)
    case l: Long => Cell(l)
    case b: BigDecimal => Cell(b)
    case b: java.math.BigDecimal => Cell(BigDecimal(b))
    case null => Cell.Empty
  }
  def autoClose[A <: AutoCloseable, B](closeable: A)(fun: (A) => B): B = {
    try {
      fun(closeable)
    } finally {
      closeable.close()
    }
  }
}
