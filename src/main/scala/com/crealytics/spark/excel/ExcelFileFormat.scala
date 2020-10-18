package com.crealytics.spark.excel

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.poi.ss.usermodel.{Cell, Row => _}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import scala.collection.AbstractIterator
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
      val workbook = reader.openWorkbook
      val allDataIterator = dataLocator.readFrom(workbook).buffered
      val headerNs = dataSchema.fieldNames.zip(allDataIterator.head).toMap
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
      val rowsIter = iter.flatMap(row => safeParser.parse(cellsExtractor.map(_.apply(row)).toArray))
      new AbstractIterator[InternalRow] {
        override def hasNext: Boolean =
          rowsIter.hasNext match {
            case true => true
            case false => workbook.close(); false
          }

        override def next(): InternalRow = rowsIter.next()
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
  private def writeObject(out: ObjectOutputStream): Unit =
    tryOrIOException {
      out.defaultWriteObject()
      value.write(out)
    }

  private def readObject(in: ObjectInputStream): Unit =
    tryOrIOException {
      value = new Configuration(false)
      value.readFields(in)
    }
}
