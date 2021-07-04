/** Copyright 2016 - 2021 Martin Mauch (@nightscape)
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
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.connector.write.WriteBuilder
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.connector.catalog.TableCapability._
import scala.collection.JavaConverters._

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

    if (files.nonEmpty) Some(infer(sparkSession, files, options)) else None
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    new ExcelWriteBuilder(paths, formatName, supportsDataType, info)

  override def supportsDataType(dataType: DataType): Boolean = true

  override def formatName: String = "Excel"

  override def fallbackFileFormat: Class[_ <: FileFormat] =
    throw new UnsupportedOperationException("Excel does not support V1 File Format")

  override def capabilities: java.util.Set[TableCapability] =
    Set(ACCEPT_ANY_SCHEMA, BATCH_READ, BATCH_WRITE, OVERWRITE_BY_FILTER, TRUNCATE).asJava

  /* Actual doing schema inferring*/
  private def infer(sparkSession: SparkSession, inputPaths: Seq[FileStatus], options: ExcelOptions): StructType = {
    val excelHelper = ExcelHelper(options)
    val excelReader = DataLocator(options)

    val workbook =
      excelHelper.getWorkbook(sparkSession.sqlContext.sparkContext.hadoopConfiguration, inputPaths.head.getPath.toUri)

    /* Depend on number of rows configured to do schema inferring*/
    val rows =
      try options.excerptSize match {
        case None => excelReader.readFrom(workbook).toSeq
        case Some(count) => excelReader.readFrom(workbook).slice(0, count).toSeq
      } finally workbook.close

    /* Ready to do schema inferring*/
    if (rows.isEmpty) StructType(Nil)
    else {
      val colNames = excelHelper.getColumnNames(rows.head)
      val nonHeaderRows = if (options.header) rows.drop(options.ignoreAfterHeader + 1) else rows

      (new ExcelInferSchema(options)).infer(nonHeaderRows, colNames)
    }
  }

}
