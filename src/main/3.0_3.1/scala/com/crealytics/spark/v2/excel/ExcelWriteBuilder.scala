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

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.execution.datasources.OutputWriterFactory
import org.apache.spark.sql.execution.datasources.v2.FileWriteBuilder
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructType

class ExcelWriteBuilder(
  paths: Seq[String],
  formatName: String,
  supportsDataType: DataType => Boolean,
  info: LogicalWriteInfo
) extends FileWriteBuilder(paths, formatName, supportsDataType, info) {
  override def prepareWrite(
    sqlConf: SQLConf,
    job: Job,
    options: Map[String, String],
    dataSchema: StructType
  ): OutputWriterFactory = {

    val excelOptions = new ExcelOptions(options, sqlConf.sessionLocalTimeZone)

    new OutputWriterFactory {
      override def newInstance(path: String, dataSchema: StructType, context: TaskAttemptContext): OutputWriter = {
        new ExcelOutputWriter(path, dataSchema, context, excelOptions)
      }

      override def getFileExtension(context: TaskAttemptContext): String =
        s".${excelOptions.fileExtension}"
    }
  }
}
