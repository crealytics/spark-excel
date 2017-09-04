package com.crealytics.spark.excel

import com.norbitltd.spoiwo.natures.xlsx.Model2XlsxConversions._
import com.norbitltd.spoiwo.model._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

class ExcelFileSaver(fs: FileSystem) {
  def save(
    location: Path,
    dataFrame: DataFrame,
    sheetName: String = "Sheet1",
    useHeader: Boolean = true
  ): Unit = {
    val headerRow = Row(dataFrame.schema.fields.map(f => Cell(f.name)))
    val dataRows = dataFrame.toLocalIterator().asScala.map { row =>
        Row(row.toSeq.map(toCell))
      }.toList
    val rows = if (useHeader) headerRow :: dataRows else dataRows
    val workbook = Sheet(name = sheetName, rows = rows).convertAsXlsx
    val outputStream = fs.create(location)
    workbook.write(outputStream)
    outputStream.hflush()
    outputStream.close()
  }
  def toCell(a: Any): Cell = a match {
    case d: java.sql.Date => Cell(
      new java.util.Date(d.getTime),
      style = CellStyle(dataFormat = CellDataFormat("yy-m-d h:mm"))
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
