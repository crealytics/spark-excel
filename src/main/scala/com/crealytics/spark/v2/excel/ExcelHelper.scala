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

package com.crealytics.spark.v2.excel

import com.github.pjfanning.xlsx.StreamingReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.poi.hssf.usermodel.HSSFWorkbookFactory
import org.apache.poi.openxml4j.util.ZipInputStreamZipEntrySource
import org.apache.poi.ss.SpreadsheetVersion
import org.apache.poi.ss.usermodel.{Cell, CellType, DataFormatter, FormulaError, Workbook, WorkbookFactory}
import org.apache.poi.ss.util.{AreaReference, CellReference}
import org.apache.poi.util.IOUtils
import org.apache.poi.xssf.usermodel.XSSFWorkbookFactory

import java.math.BigDecimal
import java.net.URI
import java.text.{FieldPosition, Format, ParsePosition}
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.Try
import scala.util.control.NonFatal

/** A format that formats a double as a plain string without rounding and scientific notation. All other operations are
  * unsupported.
  * @see
  *   [[org.apache.poi.ss.usermodel.ExcelGeneralNumberFormat]] and SSNFormat from
  *   [[org.apache.poi.ss.usermodel.DataFormatter]] from Apache POI.
  */
object PlainNumberFormat extends Format {

  override def format(number: AnyRef, toAppendTo: StringBuffer, pos: FieldPosition): StringBuffer =
    toAppendTo.append(new BigDecimal(number.toString).toPlainString)

  override def parseObject(source: String, pos: ParsePosition): AnyRef =
    throw new UnsupportedOperationException()
}

/* Excel parsing and utility methods */
class ExcelHelper private (options: ExcelOptions) {

  /* For get cell string value */
  private lazy val dataFormatter = {
    val r = new DataFormatter()
    if (options.usePlainNumberFormat) {

      /* Overwrite ExcelGeneralNumberFormat with custom PlainNumberFormat. See
       * https://github.com/crealytics/spark-excel/issues/321
       */
      val plainNumberFormat = PlainNumberFormat
      r.addFormat("General", plainNumberFormat)
      r.addFormat("@", plainNumberFormat)
    }
    r
  }

  /** Cell string value extractor, which handle difference cell type
    *
    * @param cell
    *   to be extracted
    * @return
    *   string value for given cell
    */
  def safeCellStringValue(cell: Cell): String = cell.getCellType match {
    case CellType.BLANK | CellType._NONE => ""
    case CellType.STRING => cell.getStringCellValue
    case CellType.FORMULA =>
      cell.getCachedFormulaResultType match {
        case CellType.BLANK | CellType._NONE => ""

        /* When the cell is an error-formula, and requested type is string, get error value
         */
        case CellType.ERROR => FormulaError.forInt(cell.getErrorCellValue).getString
        case CellType.STRING => cell.getStringCellValue
        case CellType.NUMERIC => cell.getNumericCellValue.toString

        /* Get what displayed on the cell, for all other cases */
        case _ => dataFormatter.formatCellValue(cell)
      }
    case _ => dataFormatter.formatCellValue(cell)
  }

  /** Get workbook
    *
    * @param conf
    *   Hadoop configuration
    * @param uri
    *   to the file, this can be on any support file system back end
    * @return
    *   workbook
    */
  def getWorkbook(conf: Configuration, uri: URI): Workbook = {
    val ins = FileSystem.get(uri, conf).open(new Path(uri))
    try {
      options.maxRowsInMemory match {
        case Some(maxRows) => {
          val builder = StreamingReader.builder().bufferSize(maxRows)
          options.workbookPassword match {
            case Some(password) => builder.password(password)
            case _ =>
          }
          builder.open(ins)
        }
        case _ => {
          options.workbookPassword match {
            case Some(password) => WorkbookFactory.create(ins, password)
            case _ => WorkbookFactory.create(ins)
          }
        }
      }
    } finally ins.close()
  }

  /** Get cell-row iterator for excel file in given URI
    *
    * @param conf
    *   Hadoop configuration
    * @param uri
    *   to the file, this can be on any support file system back end
    * @return
    *   Sheet Data with row iterator (must be closed after use)
    */
  def getSheetData(conf: Configuration, uri: URI): SheetData[Vector[Cell]] = {
    val workbook = getWorkbook(conf, uri)
    val excelReader = DataLocator(options)
    try {
      val rowIter = excelReader.readFrom(workbook)
      SheetData(rowIter, Seq(workbook))
    } catch {
      case NonFatal(t) => {
        workbook.close()
        throw t
      }
    }
  }

  /** Get cell-row iterator for excel file in given URIs
    *
    * @param conf
    *   Hadoop configuration
    * @param uris
    *   a seq of files, this can be on any support file system back end
    * @return
    *   A tuple of Sheet Data with row iterator (must be closed after use) and Vector of column names
    */
  def parseSheetData(conf: Configuration, uris: Seq[URI]): (SheetData[Vector[Cell]], Vector[String]) = {
    var sheetData = getSheetData(conf, uris.head)

    val colNames = if (sheetData.rowIterator.isEmpty) {
      Vector.empty
    } else {
      /* If the first file is empty, not checking further */
      try {
        /* Prepare field names */
        val colNames =
          if (options.header) {
            /* Get column name from the first row */
            val r = getColumnNames(sheetData.rowIterator.next)
            sheetData = sheetData.modifyIterator(_.drop(options.ignoreAfterHeader))
            r
          } else {
            /* Peek first row, then return back */
            val headerRow = sheetData.rowIterator.next
            val r = getColumnNames(headerRow)
            sheetData = sheetData.modifyIterator(iter => Iterator(headerRow) ++ iter)
            r
          }

        /* Other files also be utilized (lazily) for field types, reuse field name
           from the first file */
        val numberOfRowToIgnore = if (options.header) (options.ignoreAfterHeader + 1) else 0
        sheetData = uris.tail.foldLeft(sheetData) { case (rs, path) =>
          val newRows = getSheetData(conf, path).modifyIterator(_.drop(numberOfRowToIgnore))
          rs.append(newRows)
        }

        /* Limit numer of rows to be used for schema infering */
        options.excerptSize.foreach { excerptSize =>
          sheetData = sheetData.modifyIterator(_.take(excerptSize))
        }

        colNames
      } catch {
        case NonFatal(t) => {
          sheetData.close()
          throw t
        }
      }
    }
    (sheetData, colNames)
  }

  /** Get column name by list of cells (row)
    *
    * @param firstRow
    *   column names will be based on this
    * @return
    *   list of column names
    */
  def getColumnNames(firstRow: Vector[Cell]): Vector[String] = {

    val rowNumColumn =
      if (options.columnNameOfRowNumber.isDefined) Vector[String](options.columnNameOfRowNumber.get)
      else Vector.empty[String]

    val dataColumns =
      if (options.header) {
        val headerNames = firstRow.map(dataFormatter.formatCellValue)
        val duplicates = {
          val nonNullHeaderNames = headerNames.filter(_ != null)
          nonNullHeaderNames.groupBy(identity).filter(_._2.size > 1).keySet
        }

        firstRow.zipWithIndex.map { case (cell, index) =>
          val value = dataFormatter.formatCellValue(cell)
          val cellType = cell.getCellType
          if (
            cellType == CellType.ERROR || cellType == CellType.BLANK ||
            cellType == CellType._NONE || value.isEmpty
          ) {
            /* When there are empty strings or the, put the index as the suffix. */
            s"_c$index"
          } else if (duplicates.contains(value)) {
            /* When there are duplicates, put the index as the suffix. */
            s"$value$index"
          } else { value }
        }
      } else {
        firstRow.zipWithIndex.map { case (_, index) =>
          /* Uses default column names, "_c#" where # is its position of fields when header option is disabled.
           */
          s"_c$index"
        }
      }

    rowNumColumn ++ dataColumns
  }

  /** Get parsed range address from given ExcelOption
    *
    * @return
    *   parsed area reference
    */
  def parsedRangeAddress(): AreaReference = Try {
    val cellRef = new CellReference(options.dataAddress)
    new AreaReference(
      cellRef,
      new CellReference(
        cellRef.getSheetName,
        SpreadsheetVersion.EXCEL2007.getLastRowIndex,
        SpreadsheetVersion.EXCEL2007.getLastColumnIndex,
        false,
        false
      ),
      SpreadsheetVersion.EXCEL2007
    )
  }.getOrElse(new AreaReference(options.dataAddress, SpreadsheetVersion.EXCEL2007))

}

object ExcelHelper {

  private val configurationNeeded = new AtomicBoolean() // initializes with false
  private val configurationIsDone = new AtomicBoolean() // initializes with false

  def apply(options: ExcelOptions): ExcelHelper = {
    configureProvidersOnce() // ExcelHelper ctor is private, so we guarantee that this is called!
    options.maxByteArraySize.foreach { maxSize =>
      IOUtils.setByteArrayMaxOverride(maxSize)
    }
    options.tempFileThreshold.foreach { threshold =>
      ZipInputStreamZipEntrySource.setThresholdBytesForTempFiles(threshold)
    }
    new ExcelHelper(options)
  }

  def configureProvidersOnce(): Unit = {
    /*  initially introduced for issue #480 (PR #513) later changed to cope with
        threading issue described in PR #562

        ensures that the WorkbookFactory is initialized with the two POI providers for
        xls and xlsx. It seems that the default loading within WorkbookFactory doesn't
        work as expected. This needs to be done only once, because the providers are stored
        within a Singleton data structure.

        To avoid multi-threading issue the initialization is done by the first thread and
        all other threads running in parallel have to wait  until initialization is done.
        Otherwise the Singleton gets manipulated while another thread accesses it.
        This can lead to a java.util.ConcurrentModificationException exception while looping
        over the Singleton (see code fragment
        at org.apache.poi.ss.usermodel.WorkbookFactory.wp(WorkbookFactory.java:327).
     */
    if (!configurationIsDone.get()) {
      if (!configurationNeeded.getAndSet(true)) {
        WorkbookFactory.removeProvider(classOf[HSSFWorkbookFactory])
        WorkbookFactory.addProvider(new HSSFWorkbookFactory)

        WorkbookFactory.removeProvider(classOf[XSSFWorkbookFactory])
        WorkbookFactory.addProvider(new XSSFWorkbookFactory)
        configurationIsDone.set(true)
      } else {
        while (!configurationIsDone.get())
          Thread.sleep(100)
      }
    }
  }
}
