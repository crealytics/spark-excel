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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.ExprUtils
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.datasources.v2.TextBasedFileScan
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import scala.collection.compat.immutable.ArraySeq
import scala.jdk.CollectionConverters._

case class ExcelScan(
  sparkSession: SparkSession,
  fileIndex: PartitioningAwareFileIndex,
  dataSchema: StructType,
  readDataSchema: StructType,
  readPartitionSchema: StructType,
  options: CaseInsensitiveStringMap,
  pushedFilters: Array[Filter],
  partitionFilters: Seq[Expression] = Seq.empty,
  dataFilters: Seq[Expression] = Seq.empty
) extends TextBasedFileScan(sparkSession, options) {

  private lazy val parsedOptions: ExcelOptions = new ExcelOptions(
    options.asScala.toMap,
    sparkSession.sessionState.conf.sessionLocalTimeZone,
    sparkSession.sessionState.conf.columnNameOfCorruptRecord
  )

  override def isSplitable(path: Path): Boolean = false

  override def getFileUnSplittableReason(path: Path): String = {
    "No practical method of splitting an excel file"
  }

  override def createReaderFactory(): PartitionReaderFactory = {

    /* Check a field requirement for corrupt records here to throw an exception in a driver side
     */
    ExprUtils.verifyColumnNameOfCorruptRecord(dataSchema, parsedOptions.columnNameOfCorruptRecord)

    if (
      readDataSchema.length == 1 &&
      readDataSchema.head.name == parsedOptions.columnNameOfCorruptRecord
    ) {
      throw new RuntimeException(
        "Queries from raw Excel files are disallowed when the referenced " +
          "columns only include the internal corrupt record column"
      )
    }

    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap

    /* Hadoop Configurations are case sensitive. */
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)

    val broadcastedConf = sparkSession.sparkContext
      .broadcast(new SerializableConfiguration(hadoopConf))

    /* The partition values are already truncated in `FileScan.partitions`. We should use `readPartitionSchema` as the
     * partition schema here.
     */
    ExcelPartitionReaderFactory(
      sparkSession.sessionState.conf,
      broadcastedConf,
      dataSchema,
      readDataSchema,
      readPartitionSchema,
      parsedOptions,
      ArraySeq.unsafeWrapArray(pushedFilters)
    )
  }

  override def withFilters(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): FileScan =
    this.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)

  override def equals(obj: Any): Boolean = obj match {
    case c: ExcelScan =>
      super.equals(c) && dataSchema == c.dataSchema && options == c.options &&
      equivalentFilters(pushedFilters, c.pushedFilters)
    case _ => false
  }

  override def hashCode(): Int = super.hashCode()

  override def description(): String = {
    super.description() + ", PushedFilters: " + pushedFilters.mkString("[", ", ", "]")
  }
}
