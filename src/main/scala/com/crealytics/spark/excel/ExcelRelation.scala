package com.crealytics.spark.excel

import java.math.BigDecimal
import java.sql.{Date, Timestamp}
import java.text.NumberFormat
import java.util.Locale
import java.io._
import scala.collection.JavaConverters._
import org.apache.poi.ss.usermodel.{ WorkbookFactory, Row => SheetRow, Cell, DataFormatter, Sheet, Workbook }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.util.Try
import scala.collection.immutable.Vector

case class ExcelRelation(
  location: String,
  sheetName: Option[String],
  useHeader: Boolean,
  treatEmptyValuesAsNulls: Boolean,
  inferSheetSchema: Boolean,
  addColorColumns: Boolean = true,
  userSchema: StructType = null
  )
  (@transient val sqlContext: SQLContext)
extends BaseRelation with TableScan with PrunedScan {
  val path = new Path(location)
  val inputStream = FileSystem.get(path.toUri, sqlContext.sparkContext.hadoopConfiguration).open(path)
  val workbook = WorkbookFactory.create(inputStream)
  val sheet = findSheet(workbook, sheetName)
  val headers = getHeaders(sheet)
  override val schema: StructType = inferSchema
  val dataFormatter = new DataFormatter();

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
        val cell = row.getCell(columnIndex)
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
  private def inferSchema(): StructType = {
    if (this.userSchema != null) {
      userSchema
    } else {
      val firstRow = headers
      val header = if (useHeader) {
        firstRow.map(_.getStringCellValue)
      } else {
        firstRow.zipWithIndex.map { case (value, index) => s"C$index"}
      }
      val baseSchema = if (this.inferSheetSchema) {
        val stringsAndCellTypes = dataRows.map(_.cellIterator.asScala.map(c => c.getCellType).toVector).toVector
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
  
  /**
   * Returns the header of a sheet skipping initial empty rows.
   * 
   * @param The sheet for which the header should be returned
   * @return Vector[Cell]
   */
  private def getHeaders(sheet: Sheet): Vector[Cell] = {
    var headers: Vector[Cell] = Vector.empty
    
    var rowIndex = 0
    var isRowEmpty = true
    while(isRowEmpty) {
      if(sheet.getRow(rowIndex) != null) {
        isRowEmpty = false
        headers = 
          sheet.getRow(rowIndex).cellIterator().asScala.to[Vector]
      }
      rowIndex += 1
    }
    
    return headers
  }
}
