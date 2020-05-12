package com.crealytics.spark.excel

import java.io.InputStream

import com.crealytics.spark.excel.Utils.MapIncluding
import com.github.pjfanning.xlsx.StreamingReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.poi.ss.usermodel.{Workbook, WorkbookFactory}

trait WorkbookReader {
  def openWorkbook(): Workbook
  def withWorkbook[T](f: Workbook => T): T = {
    val workbook = openWorkbook()
    val res = f(workbook)
    // TODO: workbook.close()
    res
  }
  def sheetNames: Seq[String] = {
    withWorkbook(
      workbook =>
        for (sheetIx <- (0 until workbook.getNumberOfSheets())) yield {
          workbook.getSheetAt(sheetIx).getSheetName()
        }
    )
  }
}

object WorkbookReader {
  val WithLocationMaxRowsInMemoryAndPassword =
    MapIncluding(Seq("path"), optionally = Seq("maxRowsInMemory", "workbookPassword"))

  def apply(parameters: Map[String, String], hadoopConfiguration: Configuration): WorkbookReader = {
    def readFromHadoop(location: String) = {
      val path = new Path(location)
      FileSystem.get(path.toUri, hadoopConfiguration).open(path)
    }
    parameters match {
      case WithLocationMaxRowsInMemoryAndPassword(Seq(location), Seq(Some(maxRowsInMemory), passwordOption)) =>
        new StreamingWorkbookReader(readFromHadoop(location), passwordOption, maxRowsInMemory.toInt)
      case WithLocationMaxRowsInMemoryAndPassword(Seq(location), Seq(None, passwordOption)) =>
        new DefaultWorkbookReader(readFromHadoop(location), passwordOption)
    }
  }
}
class DefaultWorkbookReader(inputStreamProvider: => InputStream, workbookPassword: Option[String])
    extends WorkbookReader {
  def openWorkbook(): Workbook =
    workbookPassword
      .fold(WorkbookFactory.create(inputStreamProvider))(
        password => WorkbookFactory.create(inputStreamProvider, password)
      )
}

class StreamingWorkbookReader(inputStreamProvider: => InputStream, workbookPassword: Option[String], maxRowsInMem: Int)
    extends WorkbookReader {
  override def openWorkbook(): Workbook = {
    val builder = StreamingReader
      .builder()
      .rowCacheSize(maxRowsInMem)
      .bufferSize(4096)
    workbookPassword
      .fold(builder)(password => builder.password(password))
      .open(inputStreamProvider)
  }
}
