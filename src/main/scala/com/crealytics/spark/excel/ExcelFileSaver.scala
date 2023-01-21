/*
 * Copyright 2023 Martin Mauch (@nightscape)
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

package com.crealytics.spark.excel

import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.spark.sql.{DataFrame, SaveMode}
import spoiwo.model._
import spoiwo.natures.xlsx.Model2XlsxConversions._

import java.io.BufferedOutputStream
import scala.jdk.CollectionConverters._

object ExcelFileSaver {
  final val DEFAULT_SHEET_NAME = "Sheet1"
  final val DEFAULT_DATE_FORMAT = "yy-m-d h:mm"
  final val DEFAULT_TIMESTAMP_FORMAT = "yyyy-mm-dd hh:mm:ss.000"
}

class ExcelFileSaver(
  fs: FileSystem,
  location: Path,
  dataFrame: DataFrame,
  saveMode: SaveMode,
  dataLocator: DataLocator,
  header: Boolean = true
) {
  def save(): Unit = {
    def sheet(workbook: XSSFWorkbook) = {
      val headerRow = if (header) Some(dataFrame.schema.fields.map(_.name).toSeq) else None
      val dataRows = dataFrame
        .toLocalIterator()
        .asScala
        .map(_.toSeq)
      dataLocator.toSheet(headerRow, dataRows, workbook)
    }
    val fileAlreadyExists = fs.exists(location)
    def writeToWorkbook(workbook: XSSFWorkbook): Unit = {
      Workbook(sheet(workbook)).writeToExisting(workbook)
      autoClose(new BufferedOutputStream(fs.create(location)))(workbook.write)
    }
    (fileAlreadyExists, saveMode) match {
      case (false, _) | (_, SaveMode.Overwrite) =>
        if (fileAlreadyExists) {
          fs.delete(location, true)
        }
        writeToWorkbook(new XSSFWorkbook())
      case (true, SaveMode.ErrorIfExists) =>
        sys.error(s"path $location already exists.")
      case (true, SaveMode.Ignore) => ()
      case (true, SaveMode.Append) =>
        val inputStream: FSDataInputStream = fs.open(location)
        val workbook = new XSSFWorkbook(inputStream)
        inputStream.close()
        writeToWorkbook(workbook)
    }
  }

  def autoClose[A <: AutoCloseable, B](closeable: A)(fun: (A) => B): B = {
    try {
      fun(closeable)
    } finally {
      closeable.close()
    }
  }
}
