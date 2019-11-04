package com.crealytics.spark.excel

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import com.crealytics.spark.excel.ExcelFileFormat.{colName, SheetRow}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.poi.ss.usermodel.{Cell, CellType, DateUtil, Row => _}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import scala.util.control.NonFatal

class ExcelFileFormat extends FileFormat with DataSourceRegister {
  def shortName(): String = "excel"
  def prepareWrite(
    sparkSession: SparkSession,
    job: Job,
    options: Map[String, String],
    dataSchema: StructType
  ): OutputWriterFactory = {
    new OutputWriterFactory {
      override def getFileExtension(context: TaskAttemptContext): String = ".xlsx"
      override def newInstance(path: String, dataSchema: StructType, context: TaskAttemptContext): OutputWriter =
        new ExcelOutputWriter(new Path(path), dataSchema, context, new ExcelOptions(options))
    }
  }

  import ExcelFileFormat._
  override def buildReader(
    sparkSession: SparkSession,
    dataSchema: StructType,
    partitionSchema: StructType,
    requiredSchema: StructType,
    filters: Seq[Filter],
    options: Map[String, String],
    hadoopConf: Configuration
  ): PartitionedFile => Iterator[InternalRow] = {
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    val broadcastedOptions = sparkSession.sparkContext.broadcast(options)

    /*
    // Check a field requirement for corrupt records here to throw an exception in a driver side
    Try(dataSchema.fieldIndex(parsedOptions.columnNameOfCorruptRecord)).foreach { corruptFieldIndex =>
      val f = dataSchema(corruptFieldIndex)
      if (f.dataType != StringType || !f.nullable) {
        throw new RuntimeException("The field for corrupt records must be string type and nullable")
      }
    }
     */

    file: PartitionedFile => {
      val excelOptions = new ExcelOptions(broadcastedOptions.value)
      import excelOptions._
      val conf = broadcastedHadoopConf.value.value

      val reader = WorkbookReader(parameters + ("path" -> file.filePath), conf)
      val dataLocator = DataLocator(parameters)
      val excerpt: List[SheetRow] = reader.withWorkbook(dataLocator.readFrom(_).take(excerptSize).to[List])
      reader.withWorkbook { workbook =>
        val allDataIterator = dataLocator.readFrom(workbook)
        val headerNs = dataSchema.fieldNames.zip(excerpt.head).toMap
        val cellsExtractor = requiredSchema.map(
          f =>
            headerNs.get(f.name) match {
              case Some(h) => (c: SheetRow) => c.find(_.getColumnIndex == h.getColumnIndex).orNull
              case None => (_: SheetRow) => null
            }
        )

        val parser = new ExcelParser(dataSchema, requiredSchema, excelOptions)
        val safeParser = new FailureSafeParser[Array[Cell]](
          input => Seq(parser.parse(input)),
          parser.options.parseMode,
          requiredSchema,
          parser.options.columnNameOfCorruptRecord
        )
        val iter = if (headerFlag) allDataIterator.drop(1) else allDataIterator
        iter.flatMap(row => safeParser.parse(cellsExtractor.map(_.apply(row)).toArray))
      }
    }
  }

  private def sheetXHeader(excelOptions: ExcelOptions) =
    if (excelOptions.headerFlag) {
      new SheetWithHeader()
    } else {
      new SheetNoHeader()
    }

  def inferSchema(
    sparkSession: SparkSession,
    options: Map[String, String],
    files: Seq[FileStatus]
  ): Option[StructType] = {
    val excelOptions = new ExcelOptions(options)
    import excelOptions._
    val sheetX = sheetXHeader(excelOptions)
    val fieldsPerFile = files.flatMap { file: FileStatus =>
      val excerpt: List[SheetRow] =
        WorkbookReader(options + ("path" -> file.getPath.toString), sparkSession.sparkContext.hadoopConfiguration)
          .withWorkbook(DataLocator(options).readFrom(_).take(excerptSize).to[List])
      sheetX.namesAndTypes(excerpt)
    }
    val fieldForName = fieldsPerFile
      .groupBy(_.name)
      .mapValues(
        _.reduce(
          (f1, f2) =>
            f1.copy(dataType = InferSchema.inferField(f1.dataType, f2.dataType), nullable = f1.nullable || f2.nullable)
        )
      )
      .mapValues(f => if (f.dataType == NullType) f.copy(dataType = StringType) else f)
    val fieldNames = fieldsPerFile.map(_.name).distinct
    val fieldsSorted = fieldNames.map(fieldForName)
    Some(StructType(fieldsSorted))
  }
}

object ExcelFileFormat {
  def colName(cell: Cell): String = cell.getStringCellValue

  type SheetRow = Seq[Cell]
  def getSparkType(cell: Cell): DataType = {
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

}

sealed trait SheetXHeader {
  def namesAndTypes(excerpt: Seq[SheetRow]): Seq[StructField] = {
    val cellTypes: Seq[Seq[DataType]] = dataRows(excerpt)
      .map { r =>
        r.map(ExcelFileFormat.getSparkType)
      }
    val dataTypes = InferSchema(cellTypes)
    colNames(excerpt, dataTypes).zip(dataTypes).map {
      case (colName, dataType) =>
        StructField(name = colName, dataType = dataType, nullable = true)
    }
  }
  def dataRows(excerpt: Seq[SheetRow]): Seq[SheetRow]
  def colNames(excerpt: Seq[SheetRow], dataTypes: Seq[DataType]): Seq[String]
}
class SheetWithHeader() extends SheetXHeader {
  def dataRows(excerpt: Seq[SheetRow]): Seq[SheetRow] = excerpt.tail
  def colNames(excerpt: Seq[SheetRow], dataTypes: Seq[DataType]): Seq[String] = {
    require(excerpt.nonEmpty, "If headers=true, every file is required to have at least a header row")
    val headerCells = excerpt.head

    val headerNames = headerCells.map(colName)
    val duplicates = {
      val nonNullHeaderNames = headerNames.filter(_ != null)
      nonNullHeaderNames.groupBy(identity).filter(_._2.size > 1).keySet
    }
    headerCells.zipWithIndex.map {
      case (cell, index) =>
        val value = colName(cell)
        if (value == null || value.isEmpty) {
          // When there are empty strings or nulls, put the index as the suffix.
          s"_c$index"
        } else if (duplicates.contains(value)) {
          // When there are duplicates, put the index as the suffix.
          s"$value$index"
        } else {
          value
        }
    }
  }
}
class SheetNoHeader() extends SheetXHeader {

  def dataRows(excerpt: Seq[SheetRow]): Seq[SheetRow] = excerpt
  def colNames(excerpt: Seq[SheetRow], dataTypes: Seq[DataType]): Seq[String] = {
    dataTypes.indices.map { index =>
      // Uses default column names, "_c#" where # is its position of fields
      // when header option is disabled.
      s"_c$index"
    }
  }
}
class SerializableConfiguration(@transient var value: Configuration) extends Serializable {

  /**
    * Execute a block of code that returns a value, re-throwing any non-fatal uncaught
    * exceptions as IOException. This is used when implementing Externalizable and Serializable's
    * read and write methods, since Java's serializer will not report non-IOExceptions properly;
    * see SPARK-4080 for more context.
    */
  def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        // TODO: logError("Exception encountered", e)
        throw e
      case NonFatal(e) =>
        // TODO: logError("Exception encountered", e)
        throw new IOException(e)
    }
  }
  private def writeObject(out: ObjectOutputStream): Unit = tryOrIOException {
    out.defaultWriteObject()
    value.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = tryOrIOException {
    value = new Configuration(false)
    value.readFields(in)
  }
}
