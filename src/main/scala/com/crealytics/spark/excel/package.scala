package com.crealytics.spark

import com.norbitltd.spoiwo.model.Sheet
import org.apache.poi.ss.usermodel.Row.MissingCellPolicy
import org.apache.poi.ss.usermodel.{Cell, CellType, DateUtil, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrameReader, DataFrameWriter}

package object excel {
  type SheetRow = Seq[Cell]
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
        case CellType.BLANK => null
        case CellType.NUMERIC => cell.getNumericCellValue
        case CellType.STRING => cell.getStringCellValue
        case CellType.BOOLEAN => cell.getBooleanCellValue
        case CellType.FORMULA =>
          cell.getCachedFormulaResultType match {
            case CellType.BLANK => null
            case CellType.NUMERIC => cell.getNumericCellValue
            case CellType.STRING => cell.getRichStringCellValue
            case CellType.BOOLEAN => cell.getBooleanCellValue
          }
      }

    def colName: String = cell.getStringCellValue

    def sparkDataType: DataType =
      cell.getCellType match {
        case CellType.FORMULA =>
          cell.getCachedFormulaResultType match {
            case CellType.STRING => StringType
            case CellType.NUMERIC => if (DateUtil.isCellDateFormatted(cell)) TimestampType else DoubleType
            case _ => NullType
          }
        case CellType.STRING if cell.getStringCellValue == "" => NullType
        case CellType.STRING => StringType
        case CellType.BOOLEAN => BooleanType
        case CellType.NUMERIC => if (DateUtil.isCellDateFormatted(cell)) TimestampType else DoubleType
        case CellType.BLANK => NullType
      }
  }

  implicit class RichSpoiwoSheet(val sheet: Sheet) extends AnyVal {
    def extractTableData(tableNumber: Int): Seq[Seq[Any]] = {
      val table = sheet.tables(tableNumber)
      val (startRow, endRow) = table.cellRange.rowRange
      val (startColumn, endColumn) = table.cellRange.columnRange
      val tableRows = sheet.rows.filter(r => r.index.exists((startRow to endRow).contains))
      tableRows.map(_.cells.filter(_.index.exists((startColumn to endColumn).contains)).map(_.value).to[Seq])
    }
  }

  implicit class ExcelDataFrameReader(val dataFrameReader: DataFrameReader) extends AnyVal {
    def excel(
      header: Boolean = true,
      treatEmptyValuesAsNulls: Boolean = false,
      inferSchema: Boolean = false,
      addColorColumns: Boolean = false,
      dataAddress: String = null,
      timestampFormat: String = null,
      maxRowsInMemory: java.lang.Integer = null,
      excerptSize: Int = 10,
      workbookPassword: String = null
    ): DataFrameReader = {
      Map(
        "header" -> header,
        "treatEmptyValuesAsNulls" -> treatEmptyValuesAsNulls,
        "inferSchema" -> inferSchema,
        "addColorColumns" -> addColorColumns,
        "dataAddress" -> dataAddress,
        "timestampFormat" -> timestampFormat,
        "maxRowsInMemory" -> maxRowsInMemory,
        "excerptSize" -> excerptSize,
        "workbookPassword" -> workbookPassword
      ).foldLeft(dataFrameReader.format("com.crealytics.spark.excel")) {
        case (dfReader, (key, value)) =>
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
        "preHeader" -> preHeader
      ).foldLeft(dataFrameWriter.format("com.crealytics.spark.excel")) {
        case (dfWriter, (key, value)) =>
          value match {
            case null => dfWriter
            case v => dfWriter.option(key, v.toString)
          }
      }
    }
  }
}
