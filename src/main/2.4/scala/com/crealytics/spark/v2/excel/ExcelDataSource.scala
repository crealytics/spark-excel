/** Copyright 2016 - 2021 Martin Mauch (@nightscape)
  *
  * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
  * the License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */
package com.crealytics.spark.v2.excel

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.spark.crealytics.ExcelSparkInternal
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.datasources.FileStatusCache
import org.apache.spark.sql.execution.datasources.InMemoryFileIndex
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.writer._
import org.apache.spark.sql.types._

import java.net.URI
import java.util.Optional
import scala.jdk.CollectionConverters._
import java.{util => ju}

/** Creality Spark Excel data source entry point for Spark 2.4 series
  */
class ExcelDataSource extends DataSourceV2 with ReadSupport with WriteSupport with DataSourceRegister {

  private lazy val sparkSession = SparkSession.active

  /* The string that represents the format that this data source provider uses */
  override def shortName(): String = "excel"

  /** Creates a {@link DataSourceReader} to scan the data from this data source.
    *
    * If this method fails (by throwing an exception), the action will fail and no Spark job will be submitted.
    *
    * @param options
    *   the options for the returned data source reader, which is an immutable case-insensitive string-to-string map.
    */
  override def createReader(options: DataSourceOptions): DataSourceReader =
    new ExcelDataSourceReader(sparkSession, options.asMap.asScala.toMap, options.paths.toSeq, None)

  /** Creates a {@link DataSourceReader} to scan the data from this data source.
    *
    * If this method fails (by throwing an exception), the action will fail and no Spark job will be submitted.
    *
    * @param schema
    *   the user specified schema.
    * @param options
    *   the options for the returned data source reader, which is an immutable case-insensitive string-to-string map.
    */
  override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader =
    new ExcelDataSourceReader(sparkSession, options.asMap.asScala.toMap, options.paths.toSeq, Some(schema))

  /** Creates an optional {@link DataSourceWriter} to save the data to this data source. Data sources can return None if
    * there is no writing needed to be done according to the save mode.
    *
    * If this method fails (by throwing an exception), the action will fail and no Spark job will be submitted.
    *
    * @param writeUUID
    *   A unique string for the writing job. It's possible that there are many writing jobs running at the same time,
    *   and the returned {@link DataSourceWriter} can use this job id to distinguish itself from other jobs.
    * @param schema
    *   the schema of the data to be written.
    * @param mode
    *   the save mode which determines what to do when the data are already in this data source, please refer to {@link
    *   SaveMode} for more details.
    * @param options
    *   the options for the returned data source writer, which is an immutable case-insensitive string-to-string map.
    * @return
    *   a writer to append data to this data source
    */
  override def createWriter(
    jobId: String,
    schema: StructType,
    mode: SaveMode,
    options: DataSourceOptions
  ): Optional[DataSourceWriter] = {
    if (options.paths.length != 1) {
      throw new RuntimeException(s"Can only write to a single path, but ${options.paths().length} are provided")
    }
    Optional.of(new ExcelDataSourceWriter(sparkSession, options.asMap.asScala.toMap, options.paths.head, schema))
  }
}

class ExcelDataSourceReader(
  val sparkSession: SparkSession,
  val map: Map[String, String],
  val paths: Seq[String],
  userSpecifiedSchema: Option[StructType]
) extends DataSourceReader
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns {

  private lazy val options =
    new ExcelOptions(map, sparkSession.sessionState.conf.sessionLocalTimeZone)

  /** Checks and returns files in all the paths.
    * @param checkEmptyGlobPath
    * @param checkFilesExist
    */
  private def checkAndGlobPathIfNecessary(checkEmptyGlobPath: Boolean, checkFilesExist: Boolean): Seq[Path] = {
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(map)
    paths.flatMap { path =>
      val hdfsPath = new Path(path)
      val fs = hdfsPath.getFileSystem(hadoopConf)
      val qualified = hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
      val globPath = SparkHadoopUtil.get.globPathIfNecessary(fs, qualified)

      if (checkEmptyGlobPath && globPath.isEmpty) {
        throw new RuntimeException(s"Path does not exist: $qualified")
      }

      // Sufficient to check head of the globPath seq for non-glob scenario
      // Don't need to check once again if files exist in streaming mode
      if (checkFilesExist && !fs.exists(globPath.head)) {
        throw new RuntimeException(s"Path does not exist: ${globPath.head}")
      }
      globPath
    }.toSeq
  }

  lazy val fileIndex: PartitioningAwareFileIndex = {
    val rootPathsSpecified = checkAndGlobPathIfNecessary(true, true)
    val fileStatusCache = FileStatusCache.getOrCreate(sparkSession)
    new InMemoryFileIndex(sparkSession, rootPathsSpecified, map, userSpecifiedSchema, fileStatusCache)
  }

  private var _pushedFilters: Array[Filter] = Array.empty
  private var _requiredSchema: Option[StructType] = None

  lazy val dataSchema: StructType = userSpecifiedSchema match {
    case None => infer(fileIndex.allFiles())
    case Some(schema) => {
      val partitionSchema = fileIndex.partitionSchema
      val resolver = sparkSession.sessionState.conf.resolver
      StructType(schema.filterNot(f => partitionSchema.exists(p => resolver(p.name, f.name))))
    }
  }

  /** Pushes down filters, and returns filters that need to be evaluated after scanning.
    */
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    _pushedFilters = filters
    _pushedFilters
  }

  /** Returns the filters that are pushed to the data source via {@link #pushFilters(Filter[])}.
    *
    * There are 3 kinds of filters:
    *   1. pushable filters which don't need to be evaluated again after scanning. 2. pushable filters which still need
    *      to be evaluated after scanning, e.g. parquet row group filter. 3. non-pushable filters. Both case 1 and 2
    *      should be considered as pushed filters and should be returned by this method.
    *
    * It's possible that there is no filters in the query and {@link #pushFilters(Filter[])} is never called, empty
    * array should be returned for this case.
    */
  override def pushedFilters(): Array[Filter] = _pushedFilters

  /** Applies column pruning w.r.t. the given requiredSchema.
    *
    * Implementation should try its best to prune the unnecessary columns or nested fields, but it's also OK to do the
    * pruning partially, e.g., a data source may not be able to prune nested fields, and only prune top-level columns.
    *
    * Note that, data source readers should update {@link DataSourceReader#readSchema()} after applying column pruning.
    */
  override def pruneColumns(requiredSchema: StructType): Unit = {
    _requiredSchema = Some(requiredSchema)
  }

  /** Returns the actual schema of this data source reader, which may be different from the physical schema of the
    * underlying storage, as column pruning or other optimizations may happen.
    *
    * If this method fails (by throwing an exception), the action will fail and no Spark job will be submitted.
    */
  override def readSchema(): StructType = _requiredSchema match {
    case None => {
      val caseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
      SchemaUtils.checkSchemaColumnNameDuplication(dataSchema, "in the data schema", caseSensitive)

      val partitionSchema = fileIndex.partitionSchema
      SchemaUtils
        .checkSchemaColumnNameDuplication(partitionSchema, "in the partition schema", caseSensitive)
      val partitionNameSet: Set[String] = partitionSchema.fieldNames.toSet

      // When data and partition schemas have overlapping columns,
      // tableSchema = dataSchema - overlapSchema + partitionSchema
      val fields = dataSchema.fields.filterNot { field => partitionNameSet.contains(field.name) } ++
        partitionSchema.fields
      StructType(fields)
    }
    case Some(value) => value
  }

  def requiredDataSchema(): StructType = _requiredSchema match {
    case None => dataSchema
    case Some(schema) => {
      val partitionSchema = fileIndex.partitionSchema
      val resolver = sparkSession.sessionState.conf.resolver
      StructType(schema.filterNot(f => partitionSchema.exists(p => resolver(p.name, f.name))))
    }
  }

  /** Returns a list of {@link InputPartition}s. Each {@link InputPartition} is responsible for creating a data reader
    * to output data of one RDD partition. The number of input partitions returned here is the same as the number of RDD
    * partitions this scan outputs.
    *
    * Note that, this may not be a full scan if the data source reader mixes in other optimization interfaces like
    * column pruning, filter push-down, etc. These optimizations are applied before Spark issues the scan request.
    *
    * If this method fails (by throwing an exception), the action will fail and no Spark job will be submitted.
    */
  override def planInputPartitions: ju.List[InputPartition[InternalRow]] = fileIndex
    .listFiles(Seq.empty, Seq.empty)
    .map(dir =>
      dir.files.map(path =>
        new ExcelInputPartition(
          filters = _pushedFilters,
          dataSchema = dataSchema,
          requiredSchema = requiredDataSchema,
          fileIndex.partitionSchema,
          options = options,
          path = path.getPath.toUri,
          dir.values
        )
      )
    )
    .flatten
    .toList
    .asInstanceOf[List[InputPartition[InternalRow]]]
    .asJava

  /* Actual doing schema inferring */
  private def infer(inputPaths: Seq[FileStatus]): StructType = {
    val excelHelper = ExcelHelper(options)
    val conf = sparkSession.sqlContext.sparkContext.hadoopConfiguration

    /** Sampling ratio on file level (not row level as in CSV) */
    val paths = {
      var sample = (inputPaths.size * options.samplingRatio).intValue
      sample = if (sample < 1) 1 else sample
      inputPaths.take(sample).map(_.getPath.toUri)
    }
    val (sheetData, colNames) = excelHelper.parseSheetData(conf, paths)
    try {
      if (sheetData.rowIterator.isEmpty) {
        StructType(Seq.empty)
      } else {
        /* Ready to infer schema */
        ExcelInferSchema(options).infer(sheetData.rowIterator, colNames)
      }
    } finally {
      sheetData.close()
    }
  }

}

class ExcelInputPartition(
  val filters: Array[Filter],
  val dataSchema: StructType,
  val requiredSchema: StructType,
  val partitionSchema: StructType,
  val options: ExcelOptions,
  val path: URI,
  val partitionValues: InternalRow
) extends InputPartition[InternalRow] {

  /** Returns an input partition reader to do the actual reading work.
    *
    * If this method fails (by throwing an exception), the corresponding Spark task would fail and get retried until
    * hitting the maximum retry times.
    */
  override def createPartitionReader: InputPartitionReader[InternalRow] =
    new ExcelInputPartitionReader(filters, dataSchema, requiredSchema, partitionSchema, options, path, partitionValues)
}

class ExcelInputPartitionReader(
  val filters: Array[Filter],
  val dataSchema: StructType,
  val requiredSchema: StructType,
  val partitionSchema: StructType,
  val options: ExcelOptions,
  val path: URI,
  val partitionValues: InternalRow
) extends InputPartitionReader[InternalRow] {

  /* To populate get_input_filename() */
  ExcelSparkInternal.setInputFileName(path.getPath)

  private val parser = new ExcelParser(dataSchema, requiredSchema, options, filters)
  private val headerChecker =
    new ExcelHeaderChecker(dataSchema, options, source = s"Excel file: ${path}")
  private val excelHelper = ExcelHelper(options)
  private val sheetData = excelHelper.getSheetData(new Configuration(), path)

  private val reader = ExcelParser.parseIterator(sheetData.rowIterator, parser, headerChecker, dataSchema)

  private val fullSchema = requiredSchema
    .map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()) ++
    partitionSchema.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())

  // Using lazy val to avoid serialization
  private lazy val appendPartitionColumns = GenerateUnsafeProjection
    .generate(fullSchema, fullSchema)

  private val joinedRow = new JoinedRow()

  private val combinedReader = reader.map(dataRow => {
    // Using local val to avoid per-row lazy val check (pre-mature optimization?...)
    val converter = appendPartitionColumns
    converter(joinedRow(dataRow, partitionValues))
  })

  override def next: Boolean = combinedReader.hasNext
  override def get: InternalRow = combinedReader.next
  override def close(): Unit = {
    sheetData.close()
  }
}

class ExcelDataSourceWriter(
  val sparkSession: SparkSession,
  val map: Map[String, String],
  val path: String,
  val schema: StructType
) extends DataSourceWriter {

  private lazy val options =
    new ExcelOptions(map, sparkSession.sessionState.conf.sessionLocalTimeZone)

  override def createWriterFactory(): DataWriterFactory[InternalRow] =
    new ExcelDataWriterFactory(options, path, schema)

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}

class ExcelDataWriterFactory(val options: ExcelOptions, val path: String, val schema: StructType)
    extends DataWriterFactory[InternalRow] {
  override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] =
    new ExcelDataWriter(options, (new Path(path, f"part-$partitionId%06d-$taskId%09d.xlsx")).toString(), schema)
}

class ExcelDataWriter(val options: ExcelOptions, val path: String, val schema: StructType)
    extends DataWriter[InternalRow] {

  private val gen = new ExcelGenerator(path, schema, new Configuration(), options)
  if (options.header) { gen.writeHeaders() }

  override def write(record: InternalRow): Unit = gen.write(record)

  override def commit(): WriterCommitMessage = {
    gen.close()
    WriteSucceeded
  }

  override def abort(): Unit = {}

  object WriteSucceeded extends WriterCommitMessage
}
