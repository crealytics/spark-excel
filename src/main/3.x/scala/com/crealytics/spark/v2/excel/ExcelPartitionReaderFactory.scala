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

import org.apache.hadoop.conf.Configuration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import java.net.URI
import scala.util.control.NonFatal

/** A factory used to create Excel readers.
  *
  * @param sqlConf
  *   SQL configuration.
  * @param broadcastedConf
  *   Broadcasted serializable Hadoop Configuration.
  * @param dataSchema
  *   Schema of Excel files.
  * @param readDataSchema
  *   Required data schema in the batch scan.
  * @param partitionSchema
  *   Schema of partitions.
  * @param parsedOptions
  *   Options for parsing Excel files.
  */
case class ExcelPartitionReaderFactory(
  sqlConf: SQLConf,
  broadcastedConf: Broadcast[SerializableConfiguration],
  dataSchema: StructType,
  readDataSchema: StructType,
  partitionSchema: StructType,
  parsedOptions: ExcelOptions,
  filters: Seq[Filter]
) extends FilePartitionReaderFactory {

  override def buildReader(file: PartitionedFile): PartitionReader[InternalRow] = {
    val conf = broadcastedConf.value.value
    val actualDataSchema =
      StructType(dataSchema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord))
    val actualReadDataSchema =
      StructType(readDataSchema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord))
    val parser = new ExcelParser(actualDataSchema, actualReadDataSchema, parsedOptions, filters)
    val headerChecker =
      new ExcelHeaderChecker(actualReadDataSchema, parsedOptions, source = s"Excel file: ${file.filePath}")
    val iter = readFile(conf, file, parser, headerChecker, readDataSchema)
    val partitionReader = new SparkExcelPartitionReaderFromIterator(iter)
    new PartitionReaderWithPartitionValues(partitionReader, readDataSchema, partitionSchema, file.partitionValues)
  }

  private def readFile(
    conf: Configuration,
    file: PartitionedFile,
    parser: ExcelParser,
    headerChecker: ExcelHeaderChecker,
    requiredSchema: StructType
  ): SheetData[InternalRow] = {
    val excelHelper = ExcelHelper(parsedOptions)
    val sheetData = excelHelper.getSheetData(conf, URI.create(file.filePath))
    try {
      SheetData(
        ExcelParser.parseIterator(sheetData.rowIterator, parser, headerChecker, requiredSchema),
        sheetData.resourcesToClose
      )
    } catch {
      case NonFatal(t) => {
        sheetData.close()
        throw t
      }
    }
  }

}

private class SparkExcelPartitionReaderFromIterator(sheetData: SheetData[InternalRow])
    extends PartitionReaderFromIterator[InternalRow](sheetData.rowIterator) {
  override def close(): Unit = {
    super.close()
    sheetData.close()
  }
}
