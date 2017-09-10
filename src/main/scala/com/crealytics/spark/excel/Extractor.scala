package com.crealytics.spark.excel

import java.io.InputStream
import java.math.BigDecimal
import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.poi.ss.usermodel.{WorkbookFactory, Row => SheetRow, _}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

case class Extractor(useHeader: Boolean,
                     inputStream: InputStream,
                     sheetName: Option[String],
                     startColumn: Int = 0,
                     endColumn: Int = Int.MaxValue,
                     timestampFormat: Option[String] = None) {
  private lazy val workbook = WorkbookFactory.create(inputStream)
  private lazy val sheet = findSheet(workbook, sheetName)

  import com.crealytics.spark.excel.utils.RichRow._

  private val timestampParser = timestampFormat.map(d => new SimpleDateFormat(d))

  private def parseTimestamp(stringValue: String): Timestamp = {
    timestampParser match {
      case Some(parser) => new Timestamp(parser.parse(stringValue).getTime)
      case None => Timestamp.valueOf(stringValue)
    }
  }

  def firstRowWithData: Vector[Option[Cell]] = sheet.asScala
    .find(_ != null)
    .getOrElse(throw new RuntimeException(s"Sheet $sheet doesn't seem to contain any data"))
    .eachCellIterator(startColumn, endColumn)
    .to[Vector]

  def lookups(requiredColumns: Array[String],
               schema: StructType
             ): Vector[(SheetRow) => Any] = requiredColumns.map { c =>
    val columnNameRegex = s"(.*?)(_color)?".r
    val columnNameRegex(columnName, isColor) = c
    val columnIndex = schema.indexWhere(_.name == columnName)

    val cellExtractor: Cell => Any = if (isColor == null) {
      castTo(_, schema(columnIndex).dataType)
    } else {
      _.getCellStyle.getFillForegroundColorColor match {
        case null => ""
        case c: org.apache.poi.xssf.usermodel.XSSFColor => c.getARGBHex
        case uct => throw new RuntimeException(s"Unknown color type $uct: ${uct.getClass}")
      }
    }
  { row: SheetRow =>
    val cell = row.getCell(columnIndex + startColumn)
    if (cell == null) {
      null
    } else {
      cellExtractor(cell)
    }
  }
  }.to[Vector]

  private def castTo(cell: Cell, castType: DataType): Any = {
    if (cell.getCellTypeEnum == CellType.BLANK) {
      return null
    }
    val dataFormatter = new DataFormatter()
    lazy val stringValue = dataFormatter.formatCellValue(cell)
    lazy val numericValue = cell.getNumericCellValue
    lazy val bigDecimal = new BigDecimal(stringValue.replaceAll(",", ""))
    castType match {
      case _: ByteType => numericValue.toByte
      case _: ShortType => numericValue.toShort
      case _: IntegerType => numericValue.toInt
      case _: LongType => numericValue.toLong
      case _: FloatType => numericValue.toFloat
      case _: DoubleType => numericValue
      case _: BooleanType => cell.getBooleanCellValue
      case _: DecimalType => bigDecimal
      case _: TimestampType => parseTimestamp(stringValue)
      case _: DateType => new java.sql.Date(DateUtil.getJavaDate(numericValue).getTime)
      case _: StringType => stringValue
      case t => throw new RuntimeException(s"Unsupported cast from $cell to $t")
    }
  }

  private def findSheet(workBook: Workbook,
                        sheetName: Option[String]): Sheet = {
    sheetName.map { sn =>
      Option(workBook.getSheet(sn)).getOrElse(
        throw new IllegalArgumentException(s"Unknown sheet $sn")
      )
    }.getOrElse(workBook.sheetIterator.next)
  }

  def dataRows: Iterator[SheetRow] = sheet.rowIterator.asScala.drop(if (useHeader) 1 else 0)

  def extract(schema: StructType,
              requiredColumns: Array[String]): Vector[Vector[Any]] = {
    dataRows
      .map(row => lookups(requiredColumns, schema).map(l => l(row)))
      .toVector
  }

  def stringsAndCellTypes: Seq[Vector[Int]] = dataRows.map { row: SheetRow =>
    row.eachCellIterator(startColumn, endColumn).map { cell =>
      cell.fold(Cell.CELL_TYPE_BLANK)(_.getCellType)
    }.toVector
  }.toVector
}

