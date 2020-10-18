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
  private def parallelize[T : scala.reflect.ClassTag](seq: Seq[T]): RDD[T] = sqlContext.sparkContext.parallelize(seq)

  /**
    * Generates a header from the given row which is null-safe and duplicate-safe.
    */
  lazy val headerColumns: Seq[HeaderDataColumn] = {
    val firstRow = excerpt.head
    val sheetXHeader = if (header) new SheetWithHeader else new SheetNoHeader
    val fields = userSchema.getOrElse(sheetXHeader.namesAndTypes(excerpt, this.inferSheetSchema))
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
