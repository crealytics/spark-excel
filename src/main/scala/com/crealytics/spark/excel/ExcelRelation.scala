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
  header: Boolean,
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

  lazy val headerColumnForName = headerColumns.map(c => c.name -> c).toMap

  override val schema: StructType = inferSchema

  val dataFormatter = new DataFormatter()

  override def buildScan: RDD[Row] = buildScan(schema.map(_.name).toArray)

  private val timestampParser: String => Timestamp =
    timestampFormat
      .map { fmt =>
        val parser = new SimpleDateFormat(fmt)
        (stringValue: String) => new Timestamp(parser.parse(stringValue).getTime)
      }
      .getOrElse((stringValue: String) => Timestamp.valueOf(stringValue))

  val columnNameRegex = s"(?s)^(.*?)(_color)?$$".r.unanchored
  private def columnExtractor(column: String): SheetRow => Any = {
    val columnNameRegex(columnName, isColor) = column
    val headerColumn = headerColumnForName(columnName)

    val cellExtractor: PartialFunction[Seq[Cell], Any] = if (isColor == null) {
      headerColumn
    } else new ColorDataColumn(headerColumn.name, headerColumn.columnIndex)

    cellExtractor.applyOrElse(_, (_: Seq[Cell]) => null)
  }

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val lookups = requiredColumns.map(columnExtractor).toSeq
    workbookReader.withWorkbook { workbook =>
      val allDataIterator = dataLocator.readFrom(workbook)
      val iter = if (header) allDataIterator.drop(1) else allDataIterator
      val rows: Iterator[Seq[Any]] = iter
        .flatMap(
          row =>
            Try {
              val values = lookups.map(l => l(row))
              Some(values)
            }.recover {
              case e =>
                // e.printStackTrace()
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
  lazy val headerColumns: Seq[HeaderDataColumn] = {
    val firstRow = excerpt.head
    val nonHeaderRows = if (header) excerpt.tail else excerpt

    val fields = userSchema.getOrElse {
      val dataTypes = if (this.inferSheetSchema) {
        val headerIndices = firstRow.map(_.getColumnIndex)
        val cellTypes: Seq[Seq[DataType]] = nonHeaderRows
          .map { r =>
            headerIndices.map(i => r.find(_.getColumnIndex == i).map(getSparkType).getOrElse(DataTypes.NullType))
          }
        InferSchema(cellTypes)
      } else {
        // By default fields are assumed to be StringType
        excerpt.map(_.size).reduceOption(math.max) match {
          case None => Array()
          case Some(maxCellsPerRow) => {
            (0 until maxCellsPerRow).map(_ => StringType: DataType).toArray
          }
        }
      }

      def colName(cell: Cell) = cell.getStringCellValue

      val colNames = if (header) {
        val headerNames = firstRow.map(colName)
        val duplicates = {
          val nonNullHeaderNames = headerNames.filter(_ != null)
          nonNullHeaderNames.groupBy(identity).filter(_._2.size > 1).keySet
        }

        firstRow.zipWithIndex.map {
          case (cell, index) =>
            val value = colName(cell)
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
      } else {
        firstRow.zipWithIndex.map {
          case (_, index) =>
            // Uses default column names, "_c#" where # is its position of fields
            // when header option is disabled.
            s"_c$index"
        }
      }
      colNames.zip(dataTypes).map {
        case (colName, dataType) =>
          StructField(name = colName, dataType = dataType, nullable = true)
      }
    }

    firstRow.zip(fields).map {
      case (cell, field) =>
        new HeaderDataColumn(field, cell.getColumnIndex, treatEmptyValuesAsNulls, timestampParser)
    }
  }

  private def inferSchema(): StructType =
    this.userSchema.getOrElse {
      val baseSchema = StructType(headerColumns.map(_.field))
      if (addColorColumns) {
        headerColumns.foldLeft(baseSchema) { (schema, header) =>
          schema.add(s"${header.name}_color", StringType, nullable = true)
        }
      } else {
        baseSchema
      }
    }
}
