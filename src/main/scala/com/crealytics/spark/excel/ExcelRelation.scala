package com.crealytics.spark.excel

import java.math.BigDecimal
import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.poi.ss.usermodel.{
  Cell,
  CellType,
  DataFormatter,
  DateUtil,
  Sheet,
  Workbook,
  WorkbookFactory,
  Row => SheetRow
}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

case class ExcelRelation(
  location: String,
  sheetName: Option[String],
  useHeader: Boolean,
  treatEmptyValuesAsNulls: Boolean,
  inferSheetSchema: Boolean,
  addColorColumns: Boolean = true,
  userSchema: Option[StructType] = None,
  startColumn: Int = 0,
  endColumn: Int = Int.MaxValue,
  timestampFormat: Option[String] = None
)(@transient val sqlContext: SQLContext)
    extends BaseRelation
    with TableScan
    with PrunedScan {
  private val path = new Path(location)
  private val inputStream = FileSystem.get(path.toUri, sqlContext.sparkContext.hadoopConfiguration).open(path)
  private val workbook = WorkbookFactory.create(inputStream)
  private val sheet = findSheet(workbook, sheetName)
  private lazy val firstRowWithData = sheet.asScala
    .find(_ != null)
    .getOrElse(throw new RuntimeException(s"Sheet $sheet doesn't seem to contain any data"))
    .eachCellIterator(startColumn, endColumn)
    .to[Vector]

  override val schema: StructType = inferSchema
  val dataFormatter = new DataFormatter()

  val timestampParser = if (timestampFormat.isDefined) {
    Some(new SimpleDateFormat(timestampFormat.get))
  } else {
    None
  }

  private def findSheet(workBook: Workbook, sheetName: Option[String]): Sheet = {
    sheetName
      .map { sn =>
        Option(workBook.getSheet(sn)).getOrElse(throw new IllegalArgumentException(s"Unknown sheet $sn"))
      }
      .getOrElse(workBook.sheetIterator.next)
  }
  override def buildScan: RDD[Row] = buildScan(schema.map(_.name).toArray)

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val lookups = requiredColumns
      .map { c =>
        val columnNameRegex = s"(.*?)(_color)?".r
        val columnNameRegex(columnName, isColor) = c
        val columnIndex = schema.indexWhere(_.name == columnName)

        val cellExtractor: Cell => Any = if (isColor == null) {
          castTo(_, schema(columnIndex).dataType)
        } else {
          _.getCellStyle.getFillForegroundColorColor match {
            case null => ""
            case c: org.apache.poi.xssf.usermodel.XSSFColor => c.getARGBHex
            case c => throw new RuntimeException(s"Unknown color type $c: ${c.getClass}")
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
      }
      .to[Vector]
    val rows = dataRows.map(row => lookups.map(l => l(row)))
    val result = rows.to[Vector]

    sqlContext.sparkContext.parallelize(result.map(Row.fromSeq))
  }

  private def castTo(cell: Cell, castType: DataType): Any = {
    val cellType = cell.getCellTypeEnum
    if (cellType == CellType.BLANK) {
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
      case _: TimestampType =>
        cellType match {
          case CellType.NUMERIC => new Timestamp(DateUtil.getJavaDate(numericValue).getTime)
          case _ => parseTimestamp(stringValue)
        }
      case _: DateType => new java.sql.Date(DateUtil.getJavaDate(numericValue).getTime)
      case _: StringType => stringValue
      case t => throw new RuntimeException(s"Unsupported cast from $cell to $t")
    }
  }

  private def rowsRdd: RDD[SheetRow] = {
    parallelize(sheet.rowIterator().asScala.toSeq)
  }

  private def parseTimestamp(stringValue: String): Timestamp = {
    timestampParser match {
      case Some(parser) => new Timestamp(parser.parse(stringValue).getTime)
      case None => Timestamp.valueOf(stringValue)
    }
  }

  private def getSparkType(cell: Option[Cell]): DataType = {
    cell match {
      case Some(c) =>
        c.getCellType match {
          case Cell.CELL_TYPE_STRING => StringType
          case Cell.CELL_TYPE_BOOLEAN => BooleanType
          case Cell.CELL_TYPE_NUMERIC => if (DateUtil.isCellDateFormatted(c)) TimestampType else DoubleType
          case Cell.CELL_TYPE_BLANK => NullType
        }
      case None => NullType
    }
  }

  private def dataRows = sheet.rowIterator.asScala.drop(if (useHeader) 1 else 0)
  private def parallelize[T : scala.reflect.ClassTag](seq: Seq[T]): RDD[T] = sqlContext.sparkContext.parallelize(seq)
  private def inferSchema: StructType =
    this.userSchema.getOrElse {
      val header = firstRowWithData.zipWithIndex.map {
        case (Some(value), _) if useHeader => value.getStringCellValue
        case (_, index) => s"C$index"
      }
      val baseSchema = if (this.inferSheetSchema) {
        val stringsAndCellTypes = dataRows.map { row =>
          row
            .eachCellIterator(startColumn, endColumn)
            .map { cell =>
              getSparkType(cell)
            }
            .toVector
        }.toVector
        InferSchema(parallelize(stringsAndCellTypes), header.toArray)
      } else {
        // By default fields are assumed to be StringType
        val schemaFields = header.map { fieldName =>
          StructField(fieldName.toString, StringType, nullable = true)
        }
        StructType(schemaFields)
      }
      if (addColorColumns) {
        header.foldLeft(baseSchema) { (schema, header) =>
          schema.add(s"${header}_color", StringType, nullable = true)
        }
      } else {
        baseSchema
      }
    }
}
