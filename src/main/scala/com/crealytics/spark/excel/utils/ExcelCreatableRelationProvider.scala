package com.crealytics.spark.excel.utils

import com.crealytics.spark.excel.ExcelFileSaver
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

trait ExcelCreatableRelationProvider extends CreatableRelationProvider with
  SchemaRelationProvider {
  override def createRelation(
                               sqlContext: SQLContext,
                               mode: SaveMode,
                               parameters: Map[String, String],
                               data: DataFrame): BaseRelation = {
    val path = ParameterChecker.check(parameters, "path")
    val sheetName = parameters.getOrElse("sheetName", "Sheet1")
    val useHeader = ParameterChecker.check(parameters, "useHeader").toBoolean
    val timestampFormat = parameters.getOrElse("timestampFormat", ExcelFileSaver.DEFAULT_TIMESTAMP_FORMAT)
    val filesystemPath = new Path(path)
    val fs = filesystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
    val doSave = if (fs.exists(filesystemPath)) {
      mode match {
        case SaveMode.Append =>
          sys.error(s"Append mode is not supported by ${this.getClass.getCanonicalName}")
        case SaveMode.Overwrite =>
          fs.delete(filesystemPath, true)
          true
        case SaveMode.ErrorIfExists =>
          sys.error(s"path $path already exists.")
        case SaveMode.Ignore => false
      }
    } else {
      true
    }
    if (doSave) {
      // Only save data when the save mode is not ignore.
      new ExcelFileSaver(fs).save(
        filesystemPath,
        data,
        sheetName = sheetName,
        useHeader = useHeader,
        timestampFormat = timestampFormat
      )
    }

    createRelation(sqlContext, parameters, data.schema)
  }
}
