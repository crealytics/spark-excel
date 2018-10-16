package com.crealytics.spark.excel

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.poi.ss.usermodel.{Cell, CellType, DataFormatter, DateUtil, Row => _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import scala.util.Try

case class ExcelRelation(
  dataLocator: DataLocator,
  useHeader: Boolean,
  treatEmptyValuesAsNulls: Boolean,
  inferSheetSchema: Boolean,
  addColorColumns: Boolean = true,
  userSchema: Option[StructType] = None,
  timestampFormat: Option[String] = None,
  excerptSize: Int = 10,
  workbookReader: WorkbookReader
)(@transient val sqlContext: SQLContext)
    extends BaseRelation
    with TableScan
    with PrunedScan {

  type SheetRow = Seq[Cell]

  lazy val excerpt: List[SheetRow] = workbookReader.withWorkbook(dataLocator.readFrom(_).take(excerptSize).to[List])

  lazy val headerCells = excerpt.dropWhile(_.isEmpty).head
  lazy val columnIndexForName = if (useHeader) {
    headerCells.map(c => c.getStringCellValue -> c.getColumnIndex).toMap
  } else {
    schema.zipWithIndex.map { case (f, idx) => f.name -> idx }.toMap
  }

  override val schema: StructType = inferSchema

  val dataFormatter = new DataFormatter()

  override def buildScan: RDD[Row] = buildScan(schema.map(_.name).toArray)

  private val timestampParser: String => Timestamp =
    timestampFormat
      .map { fmt =>
        val parser = new SimpleDateFormat(fmt)
        (stringValue: String) =>
          new Timestamp(parser.parse(stringValue).getTime)
      }
      .getOrElse((stringValue: String) => Timestamp.valueOf(stringValue))

  val columnNameRegex = s"(?s)^(.*?)(_color)?$$".r.unanchored
  private def columnExtractor(column: String): SheetRow => Any = {
    val columnNameRegex(columnName, isColor) = column
    val columnIndex = columnIndexForName(columnName)

    val cellExtractor: PartialFunction[Seq[Cell], Any] = if (isColor == null) {
      new HeaderDataColumn(
        columnName,
        columnIndex,
        schema.find(_.name == columnName).get.dataType,
        treatEmptyValuesAsNulls,
        timestampParser
      )
    } else new ColorDataColumn(columnName, columnIndex)

    cellExtractor.applyOrElse(_, (_: Seq[Cell]) => null)
  }

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val lookups = requiredColumns.map(columnExtractor).toSeq
    workbookReader.withWorkbook { workbook =>
      val allDataIterator = dataLocator.readFrom(workbook)
      val iter = if (useHeader) allDataIterator.drop(1) else allDataIterator
      val rows: Iterator[Seq[Any]] = iter
        .flatMap(
          row =>
            Try {
              val values = lookups.map(l => l(row))
              Some(values)
            }.recover {
              case e =>
                e.printStackTrace()
                None
            }.get
        )
      val result = rows.to[Vector]
      parallelize(result.map(Row.fromSeq))
    }
  }

  private def getSparkType(cell: Cell): DataType = {
    cell.getCellType match {
      case CellType.FORMULA =>
        cell.getCachedFormulaResultType match {
          case CellType.STRING => StringType
          case CellType.NUMERIC => DoubleType
          case _ => NullType
        }
      case CellType.STRING if cell.getStringCellValue == "" => NullType
      case CellType.STRING => StringType
      case CellType.BOOLEAN => BooleanType
      case CellType.NUMERIC => if (DateUtil.isCellDateFormatted(cell)) TimestampType else DoubleType
      case CellType.BLANK => NullType
    }
  }

  private def parallelize[T : scala.reflect.ClassTag](seq: Seq[T]): RDD[T] = sqlContext.sparkContext.parallelize(seq)

  /**
    * Generates a header from the given row which is null-safe and duplicate-safe.
    */
  protected def makeSafeHeader(row: Seq[String], dataTypes: Seq[DataType]): Seq[StructField] = {
    if (useHeader) {
      val duplicates = {
        val headerNames = row
          .filter(_ != null)
        headerNames.diff(headerNames.distinct).distinct
      }

      val headerNames = row.zipWithIndex.map {
        case (value, index) =>
          if (value == null || value.isEmpty) {
            // When there are empty strings or the, put the index as the suffix.
            s"_c$index"
          } else if (duplicates.contains(value)) {
            // When there are duplicates, put the index as the suffix.
            s"$value$index"
          } else {
            value
          }
      }
      headerNames.zip(dataTypes).map { case (name, dt) => StructField(name, dt, nullable = true) }
    } else {
      dataTypes.zipWithIndex.map {
        case (dt, index) =>
          // Uses default column names, "_c#" where # is its position of fields
          // when header option is disabled.
          StructField(s"_c$index", dt, nullable = true)
      }
    }
  }

  private def inferSchema(): StructType = this.userSchema.getOrElse {
    val headerIndices = headerCells.map(_.getColumnIndex)
    val rawHeader = headerCells.map(_.getStringCellValue)

    val dataTypes = if (this.inferSheetSchema) {
      val stringsAndCellTypes: Seq[Seq[DataType]] = excerpt.tail
        .map { r =>
          headerIndices.map(i => r.find(_.getColumnIndex == i).map(getSparkType).getOrElse(DataTypes.NullType))
        }
      InferSchema(parallelize(stringsAndCellTypes))
    } else {
      // By default fields are assumed to be StringType
      val maxCellsPerRow =
        excerpt.map(_.size).reduce(math.max)
      (0 until maxCellsPerRow).map(_ => StringType: DataType).toArray
    }
    val fields = makeSafeHeader(rawHeader, dataTypes)
    val baseSchema = StructType(fields)
    if (addColorColumns) {
      fields.foldLeft(baseSchema) { (schema, header) =>
        schema.add(s"${header}_color", StringType, nullable = true)
      }
    } else {
      baseSchema
    }
  }
}
