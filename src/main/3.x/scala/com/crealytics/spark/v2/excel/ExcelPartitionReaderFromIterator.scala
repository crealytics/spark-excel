package com.crealytics.spark.v2.excel

import org.apache.hadoop.conf.Configuration
import org.apache.poi.ss.usermodel.Workbook
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.types.StructType

import java.net.URI

/** ExcelPartitionReaderFromIterator is a lightweight wrapper around spark PartitionReaderFromIterator that implements
  * the close() method, which closes the provided Excel Workbook after the read is done
  *
  * Need to be instantiated via apply() method
  */
private[excel] class ExcelPartitionReaderFromIterator[InternalRow] private (
  workbook: Workbook,
  iter: Iterator[InternalRow]
) extends PartitionReaderFromIterator[InternalRow](iter) {

  override def close(): Unit = workbook.close()

}

private[excel] object ExcelPartitionReaderFromIterator {
  def apply(
    conf: Configuration,
    parsedOptions: ExcelOptions,
    file: PartitionedFile,
    parser: ExcelParser,
    headerChecker: ExcelHeaderChecker,
    requiredSchema: StructType
  ): ExcelPartitionReaderFromIterator[InternalRow] = {
    val excelHelper = ExcelHelper(parsedOptions)

    val workbook = excelHelper.getWorkbook(conf, URI.create(file.filePath))
    val excelReader = DataLocator(parsedOptions)
    val rows = excelReader.readFrom(workbook)

    val iter = ExcelParser.parseIterator(rows, parser, headerChecker, requiredSchema)
    new ExcelPartitionReaderFromIterator[InternalRow](workbook, iter)
  }
}
