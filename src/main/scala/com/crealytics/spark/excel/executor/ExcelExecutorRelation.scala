package com.crealytics.spark.excel.executor

import com.crealytics.spark.excel.Extractor
import com.crealytics.spark.excel.utils.Loan
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SerializableWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

case class ExcelExecutorRelation(
                                  location: String,
                                  sheetName: Option[String],
                                  useHeader: Boolean,
                                  treatEmptyValuesAsNulls: Boolean,
                                  schema: StructType,
                                  startColumn: Int = 0,
                                  endColumn: Int = Int.MaxValue,
                                  timestampFormat: Option[String] = None
                                )
                                (@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with PrunedScan {

  override def buildScan: RDD[Row] = buildScan(schema.map(_.name).toArray)

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val confBroadcast = sqlContext.sparkContext.broadcast(
      new SerializableWritable(sqlContext.sparkContext.hadoopConfiguration)
    )

    sqlContext.sparkContext.parallelize(Seq(location), 1).flatMap { loc =>
      val path = new Path(loc)
      val rows = Loan.withCloseable(FileSystem.get(path.toUri, confBroadcast.value.value).open(path)) {
        inputStream =>
          Extractor(useHeader,
            inputStream,
            sheetName,
            startColumn,
            endColumn,
            timestampFormat).extract(schema, requiredColumns)
      }
      rows.map(Row.fromSeq)
    }
  }
}

