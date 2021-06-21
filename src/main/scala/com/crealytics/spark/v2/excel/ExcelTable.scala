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

import scala.collection.JavaConverters._

case class ExcelTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType]
) extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {

  override def newScanBuilder(options: CaseInsensitiveStringMap): ExcelScanBuilder =
    ExcelScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = {
    val parsedOptions =
      new ExcelOptions(options.asScala.toMap, sparkSession.sessionState.conf.sessionLocalTimeZone)

    if (files.nonEmpty) Some(infer(sparkSession, files, parsedOptions)) else None
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    new ExcelWriteBuilder(paths, formatName, supportsDataType, info)

  override def supportsDataType(dataType: DataType): Boolean = true

  override def formatName: String = "Excel"

  override def fallbackFileFormat: Class[_ <: FileFormat] =
    throw new UnsupportedOperationException("Excel does not support V1 File Format")

  /* Actual doing schema inferring*/
  private def infer(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      parsedOptions: ExcelOptions
  ): StructType = {
    val excelHelper = ExcelHelper(parsedOptions)
    val excelReader = DataLocator(parsedOptions)

    val workbook = excelHelper.getWorkbook(
      sparkSession.sqlContext.sparkContext.hadoopConfiguration,
      inputPaths.head.getPath.toUri
    )

    /* Depend on number of rows configured to do schema inferring*/
    val rows =
      try parsedOptions.excerptSize match {
        case None        => excelReader.readFrom(workbook).toSeq
        case Some(count) => excelReader.readFrom(workbook).slice(0, count).toSeq
      } finally workbook.close

    /* Ready to do schema inferring*/
    if (rows.isEmpty) StructType(Nil)
    else {
      val nonHeaderRows = if (parsedOptions.header) rows.tail else rows
      val colNames = excelHelper.getColumnNames(rows.head)

      (new ExcelInferSchema(parsedOptions)).infer(nonHeaderRows, colNames)
    }
  }

}
