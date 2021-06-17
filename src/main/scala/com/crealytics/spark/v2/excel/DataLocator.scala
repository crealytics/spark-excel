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

import org.apache.poi.ss.SpreadsheetVersion
import org.apache.poi.ss.usermodel.Cell
import org.apache.poi.ss.usermodel.Row.MissingCellPolicy
import org.apache.poi.ss.usermodel.Sheet
import org.apache.poi.ss.usermodel.Workbook
import org.apache.poi.ss.util.AreaReference
import org.apache.poi.ss.util.CellReference
import org.apache.poi.xssf.usermodel.XSSFTable
import org.apache.poi.xssf.usermodel.XSSFWorkbook

import scala.collection.JavaConverters._
import scala.util.Try

trait DataLocator {
  def readFrom(workbook: Workbook): Iterator[Vector[Cell]]
}

object DataLocator {
  def parseRangeAddress(address: String): AreaReference =
    Try {
      val cellRef = new CellReference(address)
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
    }.getOrElse(new AreaReference(address, SpreadsheetVersion.EXCEL2007))

  def apply(options: ExcelOptions): DataLocator = {
    val TableAddress = """(.*)\[(.*)\]""".r
    options.dataAddress match {
      case TableAddress(tableName, _) => new TableDataLocator(tableName)
      case _ =>
        new CellRangeAddressDataLocator(parseRangeAddress(options.dataAddress))
    }
  }
}

trait AreaDataLocator extends DataLocator {
  def columnIndices(workbook: Workbook): Range
  def rowIndices(workbook: Workbook): Range
  def sheetName(workbook: Workbook): Option[String]

  def findSheet(workBook: Workbook, sheetName: Option[String]): Sheet =
    sheetName
      .map(sn =>
        Try(Option(workBook.getSheetAt(sn.toInt))).toOption.flatten
          .orElse(Option(workBook.getSheet(sn)))
          .getOrElse(throw new IllegalArgumentException(s"Unknown sheet $sn"))
      )
      .getOrElse(workBook.getSheetAt(0))

  def readFromSheet(workbook: Workbook, name: Option[String]): Iterator[Vector[Cell]] = {
    val sheet = findSheet(workbook, name)
    val rowInd = rowIndices(workbook)
    val colInd = columnIndices(workbook)
    sheet.iterator.asScala
      .filter(r => rowInd.contains(r.getRowNum))
      .map(r =>
        colInd
          .filter(_ < r.getLastCellNum())
          .map(r.getCell(_, MissingCellPolicy.CREATE_NULL_AS_BLANK))
          .to[Vector]
      )
  }
}

class CellRangeAddressDataLocator(val dataAddress: AreaReference) extends AreaDataLocator {
  private val sheetName = Option(dataAddress.getFirstCell.getSheetName)

  def columnIndices(workbook: Workbook): Range =
    (dataAddress.getFirstCell.getCol.toInt to dataAddress.getLastCell.getCol.toInt)

  def rowIndices(workbook: Workbook): Range =
    (dataAddress.getFirstCell.getRow to dataAddress.getLastCell.getRow)

  override def readFrom(workbook: Workbook): Iterator[Vector[Cell]] =
    readFromSheet(workbook, sheetName)
  override def sheetName(workbook: Workbook): Option[String] = sheetName
}

class TableDataLocator(tableName: String) extends AreaDataLocator {
  override def readFrom(workbook: Workbook): Iterator[Vector[Cell]] = {
    val xwb = workbook.asInstanceOf[XSSFWorkbook]
    readFromSheet(workbook, Some(xwb.getTable(tableName).getSheetName))
  }

  def columnIndices(workbook: Workbook): Range =
    findTable(workbook)
      .map(t => t.getStartColIndex to t.getEndColIndex)
      .getOrElse(0 until Int.MaxValue)

  override def rowIndices(workbook: Workbook): Range =
    findTable(workbook)
      .map(t => t.getStartRowIndex to t.getEndRowIndex)
      .getOrElse(0 until Int.MaxValue)

  override def sheetName(workbook: Workbook): Option[String] =
    findTable(workbook).map(_.getSheetName).orElse(Some(tableName))

  private def findTable(workbook: Workbook): Option[XSSFTable] = {
    Option(workbook.asInstanceOf[XSSFWorkbook].getTable(tableName))
  }
}
