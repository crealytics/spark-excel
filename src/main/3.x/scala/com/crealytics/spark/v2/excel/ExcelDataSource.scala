/** Copyright 2016 - 2021 Martin Mauch (@nightscape)
  *
  * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
  * the License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */
package com.crealytics.spark.v2.excel

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.TableProvider
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._

/** Creality Spark Excel data source entry point.
  *
  * This class is heavily influenced by datasources.v2.FileDataSourceV2. We can not extends FileDataSourceV2 directly
  * because that needs a fallback implementation with V1 API for writing.
  */
class ExcelDataSource extends TableProvider with DataSourceRegister {
  import ExcelDataSource._

  private lazy val sparkSession = SparkSession.active

  private def getTableName(map: CaseInsensitiveStringMap, paths: Seq[String]): String = {
    val hadoopConf = sparkSession.sessionState
      .newHadoopConfWithOptions(map.asCaseSensitiveMap().asScala.toMap)
    shortName() + " " + paths
      .map(path => {
        val hdfsPath = new Path(path)
        val fs = hdfsPath.getFileSystem(hadoopConf)
        hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory).toString
      })
      .mkString(",")
  }

  private def getTableInternal(options: CaseInsensitiveStringMap): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    ExcelTable(tableName, sparkSession, optionsWithoutPaths, paths, None)
  }

  private def getTableInternal(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    ExcelTable(tableName, sparkSession, optionsWithoutPaths, paths, Some(schema))
  }

  private var t: Table = null

  /** The string that represents the format that this data source provider uses
    */
  override def shortName(): String = "excel"

  /** Returns true if the source has the ability of accepting external table metadata when getting tables. The external
    * table metadata includes:
    *   1. For table reader: user-specified schema from `DataFrameReader`/`DataStreamReader` and schema/partitioning
    *      stored in Spark catalog. 2. For table writer: the schema of the input `Dataframe` of
    *      `DataframeWriter`/`DataStreamWriter`.
    */
  override def supportsExternalMetadata(): Boolean = true

  /** Infer the schema of the table identified by the given options.
    *
    * @param options
    *   an immutable case-insensitive string-to-string map that can identify a table, e.g. file path, Kafka topic name,
    *   etc.
    */
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    if (t == null) t = getTableInternal(options)
    t.schema()
  }

  /** Infer the partitioning of the table identified by the given options.
    *
    * By default this method returns empty partitioning, please override it if this source support partitioning.
    *
    * @param options
    *   an immutable case-insensitive string-to-string map that can identify a table, e.g. file path, Kafka topic name,
    *   etc.
    */
  override def inferPartitioning(options: CaseInsensitiveStringMap): Array[Transform] = {
    Array.empty
  }

  /** Return a Table instance with the specified table schema, partitioning and properties to do read/write. The
    * returned table should report the same schema and partitioning with the specified ones, or Spark may fail the
    * operation.
    *
    * @param schema
    *   The specified table schema.
    * @param partitioning
    *   The specified table partitioning.
    * @param properties
    *   The specified table properties. It's case preserving (contains exactly what users specified) and implementations
    *   are free to use it case sensitively or insensitively. It should be able to identify a table, e.g. file path,
    *   Kafka topic name, etc.
    */
  override def getTable(
    schema: StructType,
    partitioning: Array[Transform],
    properties: util.Map[String, String]
  ): Table =
    if (t != null) t else getTableInternal(new CaseInsensitiveStringMap(properties), schema)

}

/* Utilities methods */
object ExcelDataSource {
  def getPaths(map: CaseInsensitiveStringMap): Seq[String] = {
    val objectMapper = new ObjectMapper()
    val paths = Option(map.get("paths"))
      .map { pathStr =>
        objectMapper.readValue(pathStr, classOf[Array[String]]).toSeq
      }
      .getOrElse(Seq.empty)
    paths ++ Option(map.get("path")).toSeq
  }

  def getOptionsWithoutPaths(map: CaseInsensitiveStringMap): CaseInsensitiveStringMap = {
    val withoutPath = map.asCaseSensitiveMap().asScala.filterKeys { k =>
      !k.equalsIgnoreCase("path") && !k.equalsIgnoreCase("paths")
    }
    new CaseInsensitiveStringMap(withoutPath.toMap.asJava)
  }

}
