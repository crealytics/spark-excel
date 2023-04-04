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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider {

  /** Creates a new relation for retrieving data from an Excel file
    */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): ExcelRelation =
    createRelation(sqlContext, parameters, null)

  /** Creates a new relation for retrieving data from an Excel file
    */
  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    schema: StructType
  ): ExcelRelation = {
    val conf = sqlContext.sparkSession.sessionState.newHadoopConf()
    val wbReader = WorkbookReader(parameters, conf)
    val dataLocator = DataLocator(parameters)
    ExcelRelation(
      header = checkParameter(parameters, "header").toBoolean,
      treatEmptyValuesAsNulls = parameters.get("treatEmptyValuesAsNulls").fold(false)(_.toBoolean),
      setErrorCellsToFallbackValues = parameters.get("setErrorCellsToFallbackValues").fold(false)(_.toBoolean),
      usePlainNumberFormat = parameters.get("usePlainNumberFormat").fold(false)(_.toBoolean),
      userSchema = Option(schema),
      inferSheetSchema = parameters.get("inferSchema").fold(false)(_.toBoolean),
      addColorColumns = parameters.get("addColorColumns").fold(false)(_.toBoolean),
      timestampFormat = parameters.get("timestampFormat"),
      excerptSize = parameters.get("excerptSize").fold(10)(_.toInt),
      dataLocator = dataLocator,
      workbookReader = wbReader
    )(sqlContext)
  }

  override def createRelation(
    sqlContext: SQLContext,
    mode: SaveMode,
    parameters: Map[String, String],
    data: DataFrame
  ): BaseRelation = {
    val path = checkParameter(parameters, "path")
    val header = checkParameter(parameters, "header").toBoolean
    val filesystemPath = new Path(path)
    val conf = sqlContext.sparkSession.sessionState.newHadoopConf()
    val fs = filesystemPath.getFileSystem(conf)
    new ExcelFileSaver(
      fs,
      filesystemPath,
      data,
      saveMode = mode,
      header = header,
      dataLocator = DataLocator(parameters)
    ).save()

    createRelation(sqlContext, parameters, data.schema)
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
