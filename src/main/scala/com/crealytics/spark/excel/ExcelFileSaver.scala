package com.crealytics.spark.excel

import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.norbitltd.spoiwo.natures.xlsx.Model2XlsxConversions._
import com.norbitltd.spoiwo.model._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

object ExcelFileSaver {
  final val DEFAULT_SHEET_NAME = "Sheet1"
  final val EXCEL_DATE_FORMAT = "yy-m-d h:mm"
  final val DEFAULT_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS"
}

class ExcelFileSaver(fs: FileSystem) {
  import ExcelFileSaver._
  def save(
    location: Path,
    dataFrame: DataFrame,
    sheetName: String = DEFAULT_SHEET_NAME,
    useHeader: Boolean = true,
    timestampFormat: String = DEFAULT_TIMESTAMP_FORMAT
  ): Unit = {
    val headerRow = Row(dataFrame.schema.fields.map(f => Cell(f.name)))
    val timestampFormatter = new SimpleDateFormat(timestampFormat)
    val dataRows = dataFrame.toLocalIterator().asScala.map { row =>
        Row(row.toSeq.map(toCell(_, timestampFormatter)))
      }.toList
    val rows = if (useHeader) headerRow :: dataRows else dataRows
    val workbook = Sheet(name = sheetName, rows = rows).convertAsXlsx
    val outputStream = fs.create(location)
    workbook.write(outputStream)
    outputStream.hflush()
    outputStream.close()
  }
  def toCell(a: Any, timestampFormatter: SimpleDateFormat): Cell = a match {
    case t: java.sql.Timestamp => Cell(timestampFormatter.format(t))
    case d: java.sql.Date => Cell(
      new java.util.Date(d.getTime),
      style = CellStyle(dataFormat = CellDataFormat(EXCEL_DATE_FORMAT))
    )
    case s: String => Cell(s)
    case d: Double => Cell(d)
    case b: Boolean => Cell(b)
    case b: Byte => Cell(b.toInt)
    case s: Short => Cell(s.toInt)
    case i: Int => Cell(i)
    case l: Long => Cell(l)
    case null => Cell.Empty
  }
}
