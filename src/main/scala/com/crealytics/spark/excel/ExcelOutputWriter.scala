package com.crealytics.spark.excel

import java.io.BufferedOutputStream

import com.crealytics.spark.excel.ExcelOutputWriter.{DEFAULT_DATE_FORMAT, DEFAULT_SHEET_NAME, DEFAULT_TIMESTAMP_FORMAT}
import com.norbitltd.spoiwo.model._
import com.norbitltd.spoiwo.natures.xlsx.Model2XlsxConversions._
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.poi.ss.util.CellRangeAddress
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types.{DataType, DateType, StructType, TimestampType, UserDefinedType}
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.JavaConverters._

object ExcelOutputWriter {
  final val DEFAULT_SHEET_NAME = "Sheet1"
  final val DEFAULT_DATE_FORMAT = "yy-m-d h:mm"
  final val DEFAULT_TIMESTAMP_FORMAT = "yyyy-mm-dd hh:mm:ss.000"
}

class ExcelOutputWriter(
  location: Path,
  schema: StructType,
  context: TaskAttemptContext,
  excelOptions: ExcelOptions,
  saveMode: SaveMode = SaveMode.Overwrite // TODO
) extends OutputWriter {

  val dataLocator = DataLocator(excelOptions.parameters)
  val fs = location.getFileSystem(context.getConfiguration)
  val headerRow = if (excelOptions.headerFlag) Some(schema.fields.map(_.name).toSeq) else None
  val dataRows: collection.mutable.Buffer[Seq[Any]] = new collection.mutable.ListBuffer[Seq[Any]]()

  def autoClose[A <: AutoCloseable, B](closeable: A)(fun: (A) => B): B = {
    try {
      fun(closeable)
    } finally {
      closeable.close()
    }
  }

  // A `ValueConverter` is responsible for converting a value of an `InternalRow` to `String`.
  // When the value is null, this converter should not be called.
  private type ValueConverter = (InternalRow, Int) => Any

  // `ValueConverter`s for all values in the fields of the schema
  private val valueConverters: Array[ValueConverter] =
    schema.map(_.dataType).map(makeConverter).toArray

  private def makeConverter(dataType: DataType): ValueConverter =
    dataType match {
      case DateType =>
        (row: InternalRow, ordinal: Int) => DateTimeUtils.toJavaDate(row.getInt(ordinal))

      case TimestampType =>
        (row: InternalRow, ordinal: Int) => DateTimeUtils.toJavaTimestamp(row.getLong(ordinal))

      case dt: DataType =>
        (row: InternalRow, ordinal: Int) => row.get(ordinal, dt)
    }

  private def convertRow(row: InternalRow): Array[Any] = {
    var i = 0
    val values = new Array[Any](row.numFields)
    while (i < row.numFields) {
      if (!row.isNullAt(i)) {
        values(i) = valueConverters(i).apply(row, i)
      } else {
        values(i) = null
      }
      i += 1
    }
    values
  }

  override def write(row: InternalRow): Unit = dataRows += convertRow(row)

  override def close(): Unit = {
    def sheet(workbook: XSSFWorkbook) = {
      dataLocator.toSheet(headerRow, dataRows.iterator, workbook)
    }

    val fileAlreadyExists = fs.exists(location)

    def writeToWorkbook(workbook: XSSFWorkbook): Unit = {
      Workbook(sheet(workbook)).writeToExisting(workbook)
      autoClose(new BufferedOutputStream(fs.create(location)))(workbook.write)
    }

    (fileAlreadyExists, saveMode) match {
      case (false, _) | (_, SaveMode.Overwrite) =>
        if (fileAlreadyExists) {
          fs.delete(location, true)
        }
        writeToWorkbook(new XSSFWorkbook())
      case (true, SaveMode.ErrorIfExists) =>
        sys.error(s"path $location already exists.")
      case (true, SaveMode.Ignore) => ()
      case (true, SaveMode.Append) =>
        val inputStream: FSDataInputStream = fs.open(location)
        val workbook = new XSSFWorkbook(inputStream)
        inputStream.close()
        writeToWorkbook(workbook)
    }
  }
}
