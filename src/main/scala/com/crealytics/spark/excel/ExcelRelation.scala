package com.crealytics.spark.excel

import java.math.BigDecimal
import java.sql.{Date, Timestamp}
import java.text.NumberFormat
import java.util.Locale

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.poi.ss.usermodel.{Cell, DataFormatter, Sheet, Workbook, WorkbookFactory, Row => SheetRow}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import scala.util.Try

case class ExcelRelation(
  location: String,
  sheetName: Option[String],
  useHeader: Boolean,
  treatEmptyValuesAsNulls: Boolean,
  inferSheetSchema: Boolean,
  addColorColumns: Boolean = true,
  userSchema: StructType = null,
  startColumn: Int = 0,
  endColumn: Int = Int.MaxValue
  )
  (@transient val sqlContext: SQLContext)
extends BaseRelation with TableScan with PrunedScan {
  private val path = new Path(location)
  private val inputStream = FileSystem.get(path.toUri, sqlContext.sparkContext.hadoopConfiguration).open(path)
  private val workbook = WorkbookFactory.create(inputStream)
  private val sheet = findSheet(workbook, sheetName)
  private lazy val firstRowWithData = sheet
    .asScala
    .find(_ != null)
    .getOrElse(throw new RuntimeException(s"Sheet $sheet doesn't seem to contain any data"))
    .eachCellIterator(startColumn, endColumn)
    .to[Vector]

  override val schema: StructType = inferSchema
  val dataFormatter = new DataFormatter()

  private def findSheet(workBook: Workbook, sheetName: Option[String]): Sheet = {
    sheetName.map { sn =>
      Option(workBook.getSheet(sn)).getOrElse(
          throw new IllegalArgumentException(s"Unknown sheet $sn")
        )
    }.getOrElse(workBook.sheetIterator.next)
  }
  override def buildScan: RDD[Row] = buildScan(schema.map(_.name).toArray)

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val lookups = requiredColumns.map {c =>
      val columnNameRegex = s"(.*?)(_color)?".r
      val columnNameRegex(columnName, isColor) = c
      val columnIndex = schema.indexWhere(_.name == columnName)

      val cellExtractor: Cell => Any = if (isColor == null) {
        { cell: Cell =>
          val value = cell.getCellType match {
            case Cell.CELL_TYPE_NUMERIC => cell.getNumericCellValue.toString
            case Cell.CELL_TYPE_BOOLEAN => cell.getBooleanCellValue.toString
            case Cell.CELL_TYPE_STRING => cell.getStringCellValue.toString
            case Cell.CELL_TYPE_BLANK => null
            case t => throw new RuntimeException(s"Unknown cell type $t for $cell")
          }
          castTo(value, schema(columnIndex).dataType)
        }
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
    }.to[Vector]
    val rows = dataRows.map(row => lookups.map(l => l(row)))
    val result = rows.to[Vector]

    sqlContext.sparkContext.parallelize(result.map(Row.fromSeq))
  }

  // Cast a String to a Spark Data Type
  private def castTo(datum: String, castType: DataType): Any = {
    if (datum == null) {
      return null
    }
    castType match {
      case _: ByteType => datum.toByte
      case _: ShortType => datum.toShort
      case _: IntegerType => datum.toInt
      case _: LongType => datum.toLong
      case _: FloatType => Try(datum.toFloat)
        .getOrElse(NumberFormat.getInstance(Locale.getDefault).parse(datum).floatValue())
      case _: DoubleType => Try(datum.toDouble)
        .getOrElse(NumberFormat.getInstance(Locale.getDefault).parse(datum).doubleValue())
      case _: BooleanType => datum.toBoolean
      case _: DecimalType => new BigDecimal(datum.replaceAll(",", ""))
      case _: TimestampType => Timestamp.valueOf(datum)
      case _: DateType => Date.valueOf(datum)
      case _: StringType => datum
      case _ => throw new RuntimeException(s"Unsupported type: ${castType.typeName}")
    }
  }

  private def rowsRdd: RDD[SheetRow] = {
    parallelize(sheet.rowIterator().asScala.toSeq)
  }

  private def dataRows = sheet.rowIterator.asScala.drop(if (useHeader) 1 else 0)
  private def parallelize[T: scala.reflect.ClassTag](seq: Seq[T]): RDD[T] = sqlContext.sparkContext.parallelize(seq)
  private def inferSchema: StructType = {
    if (this.userSchema != null) {
      userSchema
    } else {
      val header = firstRowWithData.zipWithIndex.map {
        case (Some(value), _) if useHeader => value.getStringCellValue
        case (_, index) => s"C$index"
      }
      val baseSchema = if (this.inferSheetSchema) {
        val stringsAndCellTypes = dataRows.map { row =>
          row.eachCellIterator(startColumn, endColumn).map { cell =>
            cell.fold(Cell.CELL_TYPE_BLANK)(_.getCellType)
          }.toVector
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
        header.foldLeft(baseSchema) {(schema, header) => schema.add(s"${header}_color", StringType, nullable = true)}
      } else {
        baseSchema
      }
    }
  }
}
