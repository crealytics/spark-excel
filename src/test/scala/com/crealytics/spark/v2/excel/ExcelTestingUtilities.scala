/** Copyright 2016 - 2021 Martin Mauch (@nightscape)
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
package com.crealytics.spark.v2.excel

import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

trait ExcelTestingUtilities {

  private val dataRoot = getClass.getResource("/spreadsheets").getPath

  /** Load excel data from resource folder
    *
    * @param spark spark session
    * @param path relative path to the resource/speadsheets
    * @param options extra loading option
    * @return data frame
    */
  def readFromResources(spark: SparkSession, path: String, options: Map[String, Any]): DataFrame =
    spark.read.format("excel").options(options.map(p => (p._1 -> p._2.toString())))
      .load(s"$dataRoot/$path")

  /** Load excel data from resource folder with user defined schema
    *
    * @param spark spark session
    * @param path relative path to the resource/speadsheets
    * @param options extra loading option
    * @param schema user provided schema
    * @return data frame
    */
  def readFromResources(
      spark: SparkSession,
      path: String,
      options: Map[String, Any],
      schema: StructType
  ): DataFrame = spark.read.format("excel").options(options.map(p => (p._1 -> p._2.toString())))
    .schema(schema).load(s"$dataRoot/$path")
}
