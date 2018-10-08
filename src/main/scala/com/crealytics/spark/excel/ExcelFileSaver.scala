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
  dataLocator: DataLocator,
  useHeader: Boolean = true,
  dateFormat: String = DEFAULT_DATE_FORMAT,
  timestampFormat: String = DEFAULT_TIMESTAMP_FORMAT
) {
  def save(): Unit = {
    lazy val sheet = {
      val headerRow = if (useHeader) Some(dataFrame.schema.fields.map(_.name).toSeq) else None
      val dataRows = dataFrame
        .toLocalIterator()
        .asScala
        .map(_.toSeq)
      dataLocator.toSheet(headerRow, dataRows, dateFormat, timestampFormat)
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

  def autoClose[A <: AutoCloseable, B](closeable: A)(fun: (A) => B): B = {
    try {
      fun(closeable)
    } finally {
      closeable.close()
    }
  }
}
