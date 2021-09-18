/** Copyright 2016 - 2021 Martin Mauch (@nightscape)
  *
  * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
  * the License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */
package com.crealytics.spark.v2.excel

import org.apache.hadoop.fs.Path
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.poi.ss.usermodel.Cell
import org.apache.poi.ss.usermodel.CellStyle
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._
import org.apache.poi.ss.usermodel.Workbook
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.poi.ss.util.WorkbookUtil
import org.apache.hadoop.conf.Configuration

class ExcelGenerator(val path: String, val dataSchema: StructType, val conf: Configuration, val options: ExcelOptions) {
  /* Prepare target Excel workbook, sheet and where to write to */
  private val wb: Workbook =
    if (options.fileExtension.toLowerCase == "xlsx") new XSSFWorkbook() else new HSSFWorkbook()

  private val (sheet, firstCol, firstRow) = {
    val dataAddress = ExcelHelper(options).parsedRangeAddress()
    val sheetname = {
      val ret = dataAddress.getFirstCell.getSheetName
      if (ret == null || ret.isEmpty) "Sheet1" else WorkbookUtil.createSafeSheetName(ret)
    }
    val col = dataAddress.getFirstCell.getCol.toInt
    val row = dataAddress.getFirstCell.getRow

    (wb.createSheet(sheetname), col, row)
  }

  /* Set value from Spark InternalRow to an Excel cell */
  private type ValueConverter = (InternalRow, Int, Cell) => Unit

  /* Pre prepared for each output columns based on data schema */
  private val valueConverters: Array[ValueConverter] = dataSchema.map(_.dataType).map(makeConverter).toArray

  /** Create Excel Style. This can be used inplace of cell.setType. From apache POI 5.0, cell.setType will be
    * deprecated, so this seem the only valid option after then.
    *
    * @param format
    *   string to define Excel Cell type. For examples: @, General
    * @return
    *   Excel Cell Style
    */
  private def createStyle(format: String): CellStyle = {
    val createHelper = wb.getCreationHelper();
    val r = wb.createCellStyle()
    r.setDataFormat(createHelper.createDataFormat().getFormat(format))
    r
  }

  /* Predefined Excel Cell styles/types */
  private lazy val DateCellStyle = createStyle(options.dateFormat)
  private lazy val TimestampCellStyle = createStyle(options.timestampFormat)
  private lazy val WholeNumberCellStyle = createStyle("General")
  private lazy val DecimalNumberCellStyle = createStyle("0.00E+000")
  private lazy val StringCellStyle = createStyle("@")

  private def makeConverter(dataType: DataType): ValueConverter = dataType match {
    case ByteType =>
      (row: InternalRow, ordinal: Int, cell: Cell) => {
        cell.setCellValue(row.getByte(ordinal).toDouble)
        cell.setCellStyle(WholeNumberCellStyle)
      }
    case ShortType =>
      (row: InternalRow, ordinal: Int, cell: Cell) => {
        cell.setCellValue(row.getShort(ordinal).toDouble)
        cell.setCellStyle(WholeNumberCellStyle)
      }
    case IntegerType =>
      (row: InternalRow, ordinal: Int, cell: Cell) => {
        cell.setCellValue(row.getInt(ordinal).toDouble)
        cell.setCellStyle(WholeNumberCellStyle)
      }
    case LongType =>
      (row: InternalRow, ordinal: Int, cell: Cell) => {
        cell.setCellValue(row.getLong(ordinal).toDouble)
        cell.setCellStyle(WholeNumberCellStyle)
      }
    case FloatType =>
      (row: InternalRow, ordinal: Int, cell: Cell) => {
        cell.setCellValue(row.getFloat(ordinal).toDouble)
        cell.setCellStyle(DecimalNumberCellStyle)
      }
    case DoubleType =>
      (row: InternalRow, ordinal: Int, cell: Cell) => {
        cell.setCellValue(row.getDouble(ordinal))
        cell.setCellStyle(DecimalNumberCellStyle)
      }
    case t: DecimalType =>
      (row: InternalRow, ordinal: Int, cell: Cell) => {
        cell.setCellValue(row.getDecimal(ordinal, t.precision, t.scale).toDouble)
        cell.setCellStyle(DecimalNumberCellStyle)
      }
    case DateType =>
      (row: InternalRow, ordinal: Int, cell: Cell) => {
        cell.setCellValue(new java.util.Date(row.getInt(ordinal).toLong))
        cell.setCellStyle(DateCellStyle)
      }

    case TimestampType =>
      (row: InternalRow, ordinal: Int, cell: Cell) => {
        cell.setCellValue(new java.util.Date(row.getLong(ordinal).toLong))
        cell.setCellStyle(TimestampCellStyle)
      }

    case StringType =>
      (row: InternalRow, ordinal: Int, cell: Cell) => {
        cell.setCellValue(row.getString(ordinal))
        cell.setCellStyle(StringCellStyle)
      }

    case _ => throw new RuntimeException(s"Unsupported type: ${dataType.typeName}")
  }

  /** Keep tracking of current writing row */
  private var row = firstRow

  /** Write header row */
  def writeHeaders(): Unit = {
    val excelRow = sheet.createRow(row)
    dataSchema.fields.map(_.name).zipWithIndex.map { case (name, idx) =>
      excelRow.createCell(idx + firstCol).setCellValue(name)
    }
    row += 1
  }

  /** Writes a single InternalRow to Excel using Univocity. */
  def write(record: InternalRow): Unit = {
    val excelRow = sheet.createRow(row)
    for (idx <- 0 until record.numFields) {
      val cell = excelRow.createCell(idx + firstCol)
      if (record.isNullAt(idx)) cell.setBlank() else valueConverters(idx)(record, idx, cell)
    }
    row += 1
  }

  /** Close internall workbook and flush data to the output file system. */
  def close(): Unit = {
    val fos = {
      val hdfsPath = new Path(path)
      val fs = hdfsPath.getFileSystem(conf)
      fs.create(hdfsPath, true)
    }
    wb.write(fos)
    wb.close()
    fos.close()
  }
}
