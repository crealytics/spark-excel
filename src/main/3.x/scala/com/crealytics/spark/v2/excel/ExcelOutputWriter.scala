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

import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types.StructType

class ExcelOutputWriter(path: String, dataSchema: StructType, context: TaskAttemptContext, options: ExcelOptions)
    extends OutputWriter
    with Logging {

  private val gen = new ExcelGenerator(path, dataSchema, context.getConfiguration, options)
  if (options.header) { gen.writeHeaders() }

  override def write(row: InternalRow): Unit = gen.write(row)

  override def close(): Unit = gen.close()
}
