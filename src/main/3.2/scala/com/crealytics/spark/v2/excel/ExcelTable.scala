/*
 * Copyright 2022 Martin Mauch (@nightscape)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.crealytics.spark.v2.excel

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.connector.write.WriteBuilder
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.connector.catalog.TableCapability._
import scala.jdk.CollectionConverters._

case class ExcelTable(
  name: String,
  sparkSession: SparkSession,
  map: CaseInsensitiveStringMap,
  paths: Seq[String],
  userSpecifiedSchema: Option[StructType]
) extends FileTable(sparkSession, map, paths, userSpecifiedSchema) {

  override def newScanBuilder(params: CaseInsensitiveStringMap): ExcelScanBuilder =
    ExcelScanBuilder(sparkSession, fileIndex, schema, dataSchema, params)

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = {
    val options =
      new ExcelOptions(map.asScala.toMap, sparkSession.sessionState.conf.sessionLocalTimeZone)

    if (files.nonEmpty) Some(infer(sparkSession, files, options)) else Some(StructType(Seq.empty))
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    new WriteBuilder {
      override def build(): Write = ExcelWriteBuilder(paths, formatName, supportsDataType, info)
    }

  override def supportsDataType(dataType: DataType): Boolean = true

  override def formatName: String = "Excel"

  override def fallbackFileFormat: Class[_ <: FileFormat] =
    throw new UnsupportedOperationException("Excel does not support V1 File Format")

  override def capabilities: java.util.Set[TableCapability] =
    Set(ACCEPT_ANY_SCHEMA, BATCH_READ, BATCH_WRITE, OVERWRITE_BY_FILTER, TRUNCATE).asJava

  /* Actual doing schema inferring */
  private def infer(sparkSession: SparkSession, inputPaths: Seq[FileStatus], options: ExcelOptions): StructType = {
    val excelHelper = ExcelHelper(options)
    val conf = sparkSession.sqlContext.sparkContext.hadoopConfiguration

    /* Sampling ratio on file level (not row level as in CSV) */
    val paths = {
      var sample = (inputPaths.size * options.samplingRatio).intValue
      sample = if (sample < 1) 1 else sample
      inputPaths.take(sample).map(_.getPath.toUri)
    }
    var rows = excelHelper.getRows(conf, paths.head)

    if (rows.iterator.isEmpty) { /* If the first file is empty, not checking further */
      StructType(Seq.empty)
    } else {
      try {
        /* Prepare field names */
        val colNames =
          if (options.header) {
            /* Get column name from the first row */
            val r = excelHelper.getColumnNames(rows.iterator.next())
            rows = CloseableIterator(rows.iterator.drop(options.ignoreAfterHeader), rows.resourcesToClose)
            r
          } else {
            /* Peek first row, then return back */
            val headerRow = rows.iterator.next()
            val r = excelHelper.getColumnNames(headerRow)
            rows = CloseableIterator(Iterator(headerRow) ++ rows.iterator, rows.resourcesToClose)
            r
          }

        /* Other files also be utilized (lazily) for field types, reuse field name
           from the first file */
        val numberOfRowToIgnore = if (options.header) (options.ignoreAfterHeader + 1) else 0
        paths.tail.foreach(path => {
          val newRows = excelHelper.getRows(conf, path)
          rows = CloseableIterator(
            rows.iterator ++ newRows.iterator.drop(numberOfRowToIgnore),
            rows.resourcesToClose ++ newRows.resourcesToClose
          )
        })

        /* Limit number of rows to be used for schema inferring */
        options.excerptSize.foreach { excerptSize =>
          rows = CloseableIterator(rows.iterator.take(excerptSize), rows.resourcesToClose)
        }

        /* Ready to infer schema */
        ExcelInferSchema(options).infer(rows.iterator, colNames)
      } finally {
        rows.close()
      }
    }
  }
}
