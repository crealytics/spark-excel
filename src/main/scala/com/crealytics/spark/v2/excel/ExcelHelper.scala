/** Copyright 2016 - 2021 Martin Mauch (@nightscape)
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
package com.crealytics.spark.v2.excel

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.poi.ss.usermodel.Cell
import org.apache.poi.ss.usermodel.CellType
import org.apache.poi.ss.usermodel.DataFormatter
import org.apache.poi.ss.usermodel.Workbook
import org.apache.poi.ss.usermodel.WorkbookFactory
import java.math.BigDecimal
import java.text.FieldPosition
import java.text.Format
import java.text.ParsePosition

import java.net.URI

/** A format that formats a double as a plain string without rounding and
  * scientific notation. All other operations are unsupported.
  * @see [[org.apache.poi.ss.usermodel.ExcelGeneralNumberFormat]] and SSNFormat
  * from [[org.apache.poi.ss.usermodel.DataFormatter]] from Apache POI.
  */
object PlainNumberFormat extends Format {

  override def format(number: AnyRef, toAppendTo: StringBuffer, pos: FieldPosition): StringBuffer =
    toAppendTo.append(new BigDecimal(number.toString).toPlainString)

  override def parseObject(source: String, pos: ParsePosition): AnyRef =
    throw new UnsupportedOperationException()
}

/* Excel parsing and utility methods*/
class ExcelHelper(options: ExcelOptions) {

  /* For get cell string value*/
  private lazy val dataFormatter = {
    val r = new DataFormatter()
    if (options.usePlainNumberFormat) {

      /** Overwrite ExcelGeneralNumberFormat with custom PlainNumberFormat.
        * See https://github.com/crealytics/spark-excel/issues/321
        */
      val plainNumberFormat = PlainNumberFormat
      r.addFormat("General", plainNumberFormat)
      r.addFormat("@", plainNumberFormat)
    }
    r
  }

  /** Cell string value extractor, which handle difference cell type
    *
    * @param cell to be extracted
    * @return string value for given cell
    */
  def safeCellStringValue(cell: Cell): String =
    cell.getCellType match {
      case CellType.BLANK | CellType.ERROR | CellType._NONE => ""
      case CellType.STRING => cell.getStringCellValue
      case CellType.FORMULA =>
        cell.getCachedFormulaResultType match {
          case CellType.BLANK | CellType._NONE => ""

          /** When the cell is an error-formula, and requested type is string,
            * get actual formula itself
            */
          case CellType.ERROR => dataFormatter.formatCellValue(cell)
          case CellType.STRING => cell.getStringCellValue
          case CellType.NUMERIC => cell.getNumericCellValue.toString

          /* Get what displayed on the cell, for all other cases*/
          case _ => dataFormatter.formatCellValue(cell)
        }
      case _ => dataFormatter.formatCellValue(cell)
    }

  /** Get workbook
    *
    * @param conf Hadoop configuration
    * @param uri to the file, this can be on any support file system back end
    * @param password optional password to open workbook
    * @return workbook
    */
  def getWorkbook(conf: Configuration, uri: URI): Workbook = {
    val ins = FileSystem
      .get(uri, conf)
      .open(new Path(uri))

    try options.workbookPassword match {
      case None =>
        WorkbookFactory.create(ins)
      case Some(password) =>
        WorkbookFactory.create(ins, password)
    } finally ins.close
  }

  /** Get column name by list of cells (row)
    *
    * @param firstRow column names will be based on this
    * @param options excel option
    * @return list of column names
    */
  def getColumnNames(firstRow: Vector[Cell]): Vector[String] =
    if (options.header) {
      val headerNames = firstRow.map(safeCellStringValue)
      val duplicates = {
        val nonNullHeaderNames = headerNames.filter(_ != null)
        nonNullHeaderNames.groupBy(identity).filter(_._2.size > 1).keySet
      }

      firstRow.zipWithIndex.map { case (cell, index) =>
        val value = safeCellStringValue(cell)
        if (value == null || value.isEmpty) {
          /* When there are empty strings or the, put the index as the suffix.*/
          s"_c$index"
        } else if (duplicates.contains(value)) {
          /* When there are duplicates, put the index as the suffix.*/
          s"$value$index"
        } else {
          value
        }
      }
    } else {
      firstRow.zipWithIndex.map { case (_, index) =>
        /** Uses default column names, "_c#" where # is its position of fields
          * when header option is disabled.
          */
        s"_c$index"
      }
    }
}

object ExcelHelper {
  def apply(options: ExcelOptions): ExcelHelper = new ExcelHelper(options)
}
