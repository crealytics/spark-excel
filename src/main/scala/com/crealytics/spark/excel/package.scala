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

package com.crealytics.spark

import org.apache.poi.ss.usermodel.Row.MissingCellPolicy
import org.apache.poi.ss.usermodel.{Cell, CellType, Row}
import org.apache.spark.sql.{DataFrameReader, DataFrameWriter}
import spoiwo.model.Sheet

package object excel {
  implicit class RichRow(val row: Row) extends AnyVal {
    def eachCellIterator(startColumn: Int, endColumn: Int): Iterator[Option[Cell]] =
      new Iterator[Option[Cell]] {
        private val lastCellInclusive = row.getLastCellNum - 1
        private val endCol = Math.min(endColumn, Math.max(startColumn, lastCellInclusive))
        require(startColumn >= 0 && startColumn <= endCol)

        private var nextCol = startColumn

        override def hasNext: Boolean = nextCol <= endCol && nextCol <= lastCellInclusive

        override def next(): Option[Cell] = {
          val next =
            if (nextCol > endCol) throw new NoSuchElementException(s"column index = $nextCol")
            else Option(row.getCell(nextCol, MissingCellPolicy.RETURN_NULL_AND_BLANK))
          nextCol += 1
          next
        }
      }
  }

  implicit class RichCell(val cell: Cell) extends AnyVal {
    def value: Any =
      cell.getCellType match {
        case CellType.BLANK | CellType.ERROR | CellType._NONE => null
        case CellType.NUMERIC => cell.getNumericCellValue
        case CellType.STRING => cell.getStringCellValue
        case CellType.BOOLEAN => cell.getBooleanCellValue
        case CellType.FORMULA =>
          cell.getCachedFormulaResultType match {
            case CellType.BLANK => null
            case CellType.NUMERIC => cell.getNumericCellValue
            case CellType.STRING => cell.getRichStringCellValue
            case CellType.BOOLEAN => cell.getBooleanCellValue
            case _ => null
          }
      }
  }

  implicit class RichSpoiwoSheet(val sheet: Sheet) extends AnyVal {
    def extractTableData(tableNumber: Int): Seq[Seq[Any]] = {
      val table = sheet.tables(tableNumber)
      val (startRow, endRow) = table.cellRange.rowRange
      val (startColumn, endColumn) = table.cellRange.columnRange
      val tableRows = sheet.rows.filter(r => r.index.exists((startRow to endRow).contains))
      tableRows.map(_.cells.filter(_.index.exists((startColumn to endColumn).contains)).map(_.value).toSeq)
    }
  }

  implicit class ExcelDataFrameReader(val dataFrameReader: DataFrameReader) extends AnyVal {
    def excel(
      header: Boolean = true,
      treatEmptyValuesAsNulls: Boolean = false,
      setErrorCellsToFallbackValues: Boolean = false,
      inferSchema: Boolean = false,
      usePlainNumberFormat: Boolean = false,
      addColorColumns: Boolean = false,
      dataAddress: String = null,
      timestampFormat: String = null,
      maxRowsInMemory: java.lang.Integer = null,
      excerptSize: Int = 10,
      workbookPassword: String = null,
      maxByteArraySize: java.lang.Integer = null
    ): DataFrameReader = {
      Map(
        "header" -> header,
        "treatEmptyValuesAsNulls" -> treatEmptyValuesAsNulls,
        "setErrorCellsToFallbackValues" -> setErrorCellsToFallbackValues,
        "usePlainNumberFormat" -> usePlainNumberFormat,
        "inferSchema" -> inferSchema,
        "addColorColumns" -> addColorColumns,
        "dataAddress" -> dataAddress,
        "timestampFormat" -> timestampFormat,
        "maxRowsInMemory" -> maxRowsInMemory,
        "excerptSize" -> excerptSize,
        "workbookPassword" -> workbookPassword,
        "maxByteArraySize" -> maxByteArraySize
      ).foldLeft(dataFrameReader.format("com.crealytics.spark.excel")) { case (dfReader, (key, value)) =>
        value match {
          case null => dfReader
          case v => dfReader.option(key, v.toString)
        }
      }
    }
  }

  implicit class ExcelDataFrameWriter[T](val dataFrameWriter: DataFrameWriter[T]) extends AnyVal {
    def excel(
      header: Boolean = true,
      dataAddress: String = null,
      preHeader: String = null,
      dateFormat: String = null,
      timestampFormat: String = null,
      workbookPassword: String = null
    ): DataFrameWriter[T] = {
      Map(
        "header" -> header,
        "dataAddress" -> dataAddress,
        "dateFormat" -> dateFormat,
        "timestampFormat" -> timestampFormat,
        "workbookPassword" -> workbookPassword,
        "preHeader" -> preHeader
      ).foldLeft(dataFrameWriter.format("com.crealytics.spark.excel")) { case (dfWriter, (key, value)) =>
        value match {
          case null => dfWriter
          case v => dfWriter.option(key, v.toString)
        }
      }
    }
  }
}
