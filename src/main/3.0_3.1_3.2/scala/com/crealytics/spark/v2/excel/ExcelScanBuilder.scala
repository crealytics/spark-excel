package com.crealytics.spark.v2.excel

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Scan, SupportsPushDownFilters}
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class ExcelScanBuilder(
  sparkSession: SparkSession,
  fileIndex: PartitioningAwareFileIndex,
  schema: StructType,
  dataSchema: StructType,
  options: CaseInsensitiveStringMap
) extends FileScanBuilder(sparkSession, fileIndex, dataSchema)
    with SupportsPushDownFilters {

  override def build(): Scan = {
    ExcelScan(sparkSession, fileIndex, dataSchema, readDataSchema(), readPartitionSchema(), options, pushedFilters())
  }

  private var _pushedFilters: Array[Filter] = Array.empty

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    _pushedFilters = ExcelFilters.pushedFilters(filters, dataSchema)
    filters
  }

  override def pushedFilters(): Array[Filter] = _pushedFilters
}
