package com.crealytics.spark.excel

import com.crealytics.spark.excel.Utils._
import org.apache.hadoop.fs.Path
import org.apache.poi.ss.util.{CellRangeAddress, CellReference}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.{FileIndex, HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.util.Try

class DefaultSource extends CreatableRelationProvider with DataSourceRegister {
  def shortName(): String = "excel-single-file"

  override def createRelation(
    sqlContext: SQLContext,
    mode: SaveMode,
    parameters: Map[String, String],
    data: DataFrame
  ): BaseRelation = {
    val path = checkParameter(parameters, "path")
    val header = checkParameter(parameters, "header").toBoolean
    val filesystemPath = new Path(path)
    val fs = filesystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
    new ExcelFileSaver(
      fs,
      filesystemPath,
      data,
      saveMode = mode,
      header = header,
      dataLocator = DataLocator(parameters)
    ).save()

    HadoopFsRelation(
      location = new FileIndex {
        override def rootPaths: Seq[Path] = ???

        override def listFiles(
          partitionFilters: Seq[Expression],
          dataFilters: Seq[Expression]
        ): Seq[PartitionDirectory] = ???

        override def inputFiles: Array[String] = Array(path)

        override def refresh(): Unit = ???

        override def sizeInBytes: Long = fs.getContentSummary(filesystemPath).getLength

        override def partitionSchema: StructType = ???
      },
      partitionSchema = StructType(Array.empty[StructField]),
      dataSchema = data.schema,
      bucketSpec = None,
      fileFormat = new ExcelFileFormat,
      options = parameters
    )(sqlContext.sparkSession)
  }

  // Forces a Parameter to exist, otherwise an exception is thrown.
  private def checkParameter(map: Map[String, String], param: String): String = {
    if (!map.contains(param)) {
      throw new IllegalArgumentException(s"Parameter ${'"'}$param${'"'} is missing in options.")
    } else {
      map.apply(param)
    }
  }
}
