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

package com.crealytics.spark.excel.v2

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriter, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types._

/** derived from binary file data source. Needed to support writing excel using the V2 API
  */
class ExcelFileFormat extends FileFormat with DataSourceRegister {

  override def inferSchema(
    sparkSession: SparkSession,
    options: Map[String, String],
    files: Seq[FileStatus]
  ): Option[StructType] = {
    throw new UnsupportedOperationException("ExcelFileFormat as fallback format for V2 supports writing only")
  }

  override def prepareWrite(
    sparkSession: SparkSession,
    job: Job,
    options: Map[String, String],
    dataSchema: StructType
  ): OutputWriterFactory = {
    val excelOptions = new ExcelOptions(options, sparkSession.conf.get("spark.sql.session.timeZone"))

    new OutputWriterFactory {
      override def newInstance(path: String, dataSchema: StructType, context: TaskAttemptContext): OutputWriter = {
        new ExcelOutputWriter(path, dataSchema, context, excelOptions)
      }

      override def getFileExtension(context: TaskAttemptContext): String =
        s".${excelOptions.fileExtension}"
    }
  }

  override def isSplitable(sparkSession: SparkSession, options: Map[String, String], path: Path): Boolean = {
    false
  }

  override def shortName(): String = "excel"

  /*
  We need this class for writing only, thus reader is not implemented
   */
  override protected def buildReader(
    sparkSession: SparkSession,
    dataSchema: StructType,
    partitionSchema: StructType,
    requiredSchema: StructType,
    filters: Seq[Filter],
    options: Map[String, String],
    hadoopConf: Configuration
  ): PartitionedFile => Iterator[InternalRow] = {
    throw new UnsupportedOperationException("ExcelFileFormat as fallback format for V2 supports writing only")
  }

}
