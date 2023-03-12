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

package com.crealytics.spark.excel.v2

import org.apache.poi.ss.usermodel.{Cell, Sheet, Workbook}
import org.apache.poi.ss.usermodel.Row.MissingCellPolicy
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.spark.internal.Logging

import scala.jdk.CollectionConverters._
import scala.util.Try

/** For handling Excel data address and read data from there */
trait DataLocator {

  /** Get cell-row itertor for given workbook with parsed address from option
    *
    * @param workbook
    *   to be create iterator for
    * @return
    *   cell-row iterator
    */
  def readFrom(workbook: Workbook): Iterator[Vector[Cell]]

  def actualReadFromSheet(options: ExcelOptions, sheet: Sheet, rowInd: Range, colInd: Range): Iterator[Vector[Cell]] = {
    if (options.keepUndefinedRows) {
      rowInd.iterator.map(rid => {
        val r = sheet.getRow(rid)
        if (r == null) { Vector.empty }
        else {
          val lastCellNum = r.getLastCellNum
          colInd
            .takeWhile(_ < lastCellNum)
            .map(r.getCell(_, MissingCellPolicy.CREATE_NULL_AS_BLANK))
            .toVector
        }
      })

    } else {
      sheet.iterator.asScala
        .filter(r => rowInd.contains(r.getRowNum))
        .map { r =>
          val lastCellNum = r.getLastCellNum
          colInd
            .takeWhile(_ < lastCellNum)
            .map(r.getCell(_, MissingCellPolicy.CREATE_NULL_AS_BLANK))
            .toVector
        }
    }
  }
}

object DataLocator {
  def apply(options: ExcelOptions): DataLocator = {
    val tableAddressRegex = """(.*)\[(.*)\]""".r
    options.dataAddress match {
      case tableAddressRegex(tableName, _) => new TableDataLocator(options, tableName)
      case _ => new CellRangeAddressDataLocator(options)
    }
  }
}

/** Locating the data in Excel Range Address
  *
  * @param options
  *   user specified excel option
  */
class CellRangeAddressDataLocator(val options: ExcelOptions) extends DataLocator {

  override def readFrom(workbook: Workbook): Iterator[Vector[Cell]] = {
    val sheet = findSheet(workbook, sheetName)
    val rowInd = rowIndices(sheet)
    val colInd = columnIndices()

    actualReadFromSheet(options, sheet, rowInd, colInd)
  }

  private def findSheet(workbook: Workbook, name: Option[String]): Sheet = name
    .map(sn =>
      Try(Option(workbook.getSheetAt(sn.toInt))).toOption.flatten
        .orElse(Option(workbook.getSheet(sn)))
        .getOrElse(throw new IllegalArgumentException(s"Unknown sheet $sn"))
    )
    .getOrElse(workbook.getSheetAt(0))

  private val dataAddress = ExcelHelper(options).parsedRangeAddress()

  private val sheetName = Option(dataAddress.getFirstCell.getSheetName)

  private def columnIndices(): Range =
    dataAddress.getFirstCell.getCol.toInt to dataAddress.getLastCell.getCol.toInt

  private def rowIndices(sheet: Sheet): Range =
    (math.max(dataAddress.getFirstCell.getRow, sheet.getFirstRowNum) to
      math.min(dataAddress.getLastCell.getRow, sheet.getLastRowNum))
}

/** Locating the data in Excel Table
  *
  * @param options
  *   user specified excel option
  */
class TableDataLocator(val options: ExcelOptions, tableName: String) extends DataLocator with Logging {
  override def readFrom(workbook: Workbook): Iterator[Vector[Cell]] = {
    workbook match {
      case xssfWorkbook: XSSFWorkbook => {
        val table = xssfWorkbook.getTable(tableName)
        val sheet = table.getXSSFSheet()
        val rowInd = (table.getStartRowIndex to table.getEndRowIndex)
        val colInd = (table.getStartColIndex to table.getEndColIndex)

        actualReadFromSheet(options, sheet, rowInd, colInd)
      }
      case _ => {
        logWarning("TableDataLocator only properly supports xlsx files read without maxRowsInMemory setting")
        List.empty.iterator
      }
    }
  }
}
