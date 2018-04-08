package com.crealytics.spark.excel

import java.io.BufferedOutputStream
import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.norbitltd.spoiwo.natures.xlsx.Model2XlsxConversions._
import com.norbitltd.spoiwo.model._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

object ExcelFileSaver {
  final val DEFAULT_SHEET_NAME = "Sheet1"
  final val DEFAULT_DATE_FORMAT = "yy-m-d h:mm"
  final val DEFAULT_TIMESTAMP_FORMAT = "yyyy-mm-dd hh:mm:ss.000"
}

class ExcelFileSaver(fs: FileSystem) {
  import ExcelFileSaver._
  def save(
    location: Path,
    dataFrame: DataFrame,
    sheetName: String = DEFAULT_SHEET_NAME,
    useHeader: Boolean = true,
    dateFormat: String = DEFAULT_DATE_FORMAT,
    timestampFormat: String = DEFAULT_TIMESTAMP_FORMAT
  ): Unit = {
    val headerRow = Row(dataFrame.schema.fields.map(f => Cell(f.name)))
    val dataRows = dataFrame
      .toLocalIterator()
      .asScala
      .map { row =>
        Row(row.toSeq.map(toCell(_, dateFormat, timestampFormat)))
      }
      .toList
    val rows = if (useHeader) headerRow :: dataRows else dataRows
    val workbook = Sheet(name = sheetName, rows = rows).convertAsXlsx
    autoClose(new BufferedOutputStream(fs.create(location)))(workbook.write)
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
