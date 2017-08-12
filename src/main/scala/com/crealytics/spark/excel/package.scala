package com.crealytics.spark

import org.apache.poi.ss.usermodel.Row.MissingCellPolicy
import org.apache.poi.ss.usermodel.{Cell, Row}

package object excel {
  val LOCATION_KEY = "location"
  val SHEET_NAME_KEY = "sheetName"
  val USE_HEADER_KEY = "useHeader"
  val TREAT_EMPTY_VALUES_AS_NULL_KEY = "treatEmptyValuesAsNulls"
  val INFER_SCHEMA_KEY = "inferSchema"
  val ADD_COLOR_COLUMNS_KEY = "addColorColumns"
  val START_COLUMN_KEY = "startColumn"
  val END_COLUMN_KEY = "endColumn"

  implicit class RichRow(val row: Row) extends AnyVal {

    def eachCellIterator(startColumn: Int, endColumn: Int): Iterator[Option[Cell]] = new Iterator[Option[Cell]] {
      private val lastCellInclusive = row.getLastCellNum - 1
      private val endCol = Math.min(endColumn, Math.max(startColumn, lastCellInclusive))
      require(startColumn >= 0 && startColumn <= endCol)

      private var nextCol = startColumn

      override def hasNext: Boolean = nextCol <= endCol && nextCol <= lastCellInclusive

      override def next(): Option[Cell] = {
        val next = if (nextCol > endCol) throw new NoSuchElementException(s"column index = $nextCol")
          else Option(row.getCell(nextCol, MissingCellPolicy.RETURN_NULL_AND_BLANK))
        nextCol += 1
        next
      }
    }

  }

}
