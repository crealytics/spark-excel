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

package com.crealytics.spark.excel

import java.io.InputStream
import com.crealytics.spark.excel.Utils.MapIncluding
import com.github.pjfanning.xlsx.StreamingReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.poi.ss.usermodel.{Workbook, WorkbookFactory}
import org.apache.poi.hssf.usermodel.HSSFWorkbookFactory
import org.apache.poi.openxml4j.util.ZipInputStreamZipEntrySource
import org.apache.poi.util.IOUtils
import org.apache.poi.xssf.usermodel.XSSFWorkbookFactory
import scala.collection.JavaConverters.mapAsScalaMapConverter

trait WorkbookReader {
  protected def openWorkbook(): Workbook
  def withWorkbook[T](f: Workbook => T): T = {
    val workbook = openWorkbook()
    val res = f(workbook)
    workbook.close()
    res
  }
  def sheetNames: Seq[String] = {
    withWorkbook(workbook =>
      for (sheetIx <- (0 until workbook.getNumberOfSheets())) yield {
        workbook.getSheetAt(sheetIx).getSheetName()
      }
    )
  }
}

object WorkbookReader {
  val WithLocationMaxRowsInMemoryAndPassword =
    MapIncluding(
      Seq("path"),
      optionally = Seq("maxRowsInMemory", "workbookPassword", "maxByteArraySize", "tempFileThreshold")
    )

  WorkbookFactory.addProvider(new HSSFWorkbookFactory)
  WorkbookFactory.addProvider(new XSSFWorkbookFactory)

  def apply(parameters: java.util.HashMap[String, String], hadoopConfiguration: Configuration): WorkbookReader = {
    apply(parameters.asScala.toMap, hadoopConfiguration)
  }

  def apply(parameters: Map[String, String], hadoopConfiguration: Configuration): WorkbookReader = {
    def readFromHadoop(location: String) = {
      val path = new Path(location)
      FileSystem.get(path.toUri, hadoopConfiguration).open(path)
    }
    parameters match {
      case WithLocationMaxRowsInMemoryAndPassword(
            Seq(location),
            Seq(Some(maxRowsInMemory), passwordOption, maxByteArraySizeOption, tempFileThreshold)
          ) =>
        new StreamingWorkbookReader(
          readFromHadoop(location),
          passwordOption,
          maxRowsInMemory.toInt,
          maxByteArraySizeOption.map(_.toInt),
          tempFileThreshold.map(_.toInt)
        )
      case WithLocationMaxRowsInMemoryAndPassword(
            Seq(location),
            Seq(None, passwordOption, maxByteArraySizeOption, tempFileThresholdOption)
          ) =>
        new DefaultWorkbookReader(
          readFromHadoop(location),
          passwordOption,
          maxByteArraySizeOption.map(_.toInt),
          tempFileThresholdOption.map(_.toInt)
        )
    }
  }
}
class DefaultWorkbookReader(
  inputStreamProvider: => InputStream,
  workbookPassword: Option[String],
  maxByteArraySize: Option[Int],
  tempFileThreshold: Option[Int]
) extends WorkbookReader {

  protected def openWorkbook(): Workbook = {
    maxByteArraySize.foreach { maxSize =>
      IOUtils.setByteArrayMaxOverride(maxSize)
    }
    tempFileThreshold.foreach { threshold =>
      ZipInputStreamZipEntrySource.setThresholdBytesForTempFiles(threshold)
    }
    workbookPassword
      .fold(WorkbookFactory.create(inputStreamProvider))(password =>
        WorkbookFactory.create(inputStreamProvider, password)
      )
  }
}

class StreamingWorkbookReader(
  inputStreamProvider: => InputStream,
  workbookPassword: Option[String],
  maxRowsInMem: Int,
  maxByteArraySize: Option[Int],
  tempFileThreshold: Option[Int]
) extends WorkbookReader {
  override protected def openWorkbook(): Workbook = {
    maxByteArraySize.foreach { maxSize =>
      IOUtils.setByteArrayMaxOverride(maxSize)
    }
    tempFileThreshold.foreach { threshold =>
      ZipInputStreamZipEntrySource.setThresholdBytesForTempFiles(threshold)
    }
    val builder = StreamingReader
      .builder()
      .rowCacheSize(maxRowsInMem)
      .bufferSize(4096)
    workbookPassword
      .fold(builder)(password => builder.password(password))
      .open(inputStreamProvider)
  }
}
