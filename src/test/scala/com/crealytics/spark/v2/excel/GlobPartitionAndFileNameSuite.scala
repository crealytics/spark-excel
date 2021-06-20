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

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql._
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

import java.nio.file.Paths

/** Issue References:
  *
  * #52. input_file_name returns empty string
  *      https://github.com/crealytics/spark-excel/issues/52
  *
  * #74. Allow reading multiple files specified as a list OR by a pattern
  *      https://github.com/crealytics/spark-excel/issues/74
  *
  * #97. Reading multiple files
  *      https://github.com/crealytics/spark-excel/issues/97
  */

object GlobPartitionAndFileNameSuite {
  val expectedInferredSchema = StructType(
    List(
      StructField("Day", DoubleType, true),
      StructField("Month", DoubleType, true),
      StructField("Customer ID", StringType, true),
      StructField("Customer Name", StringType, true),
      StructField("Standard Package", DoubleType, true),
      StructField("Extra Option 1", DoubleType, true),
      StructField("Extra Option 2", DoubleType, true),
      StructField("Extra Option 3", DoubleType, true),
      StructField("Staff", StringType, true)
    )
  )

  val expectedWithFilenameSchema = StructType(
    List(
      StructField("Day", DoubleType, true),
      StructField("Month", DoubleType, true),
      StructField("Customer ID", StringType, true),
      StructField("Customer Name", StringType, true),
      StructField("Standard Package", DoubleType, true),
      StructField("Extra Option 1", DoubleType, true),
      StructField("Extra Option 2", DoubleType, true),
      StructField("Extra Option 3", DoubleType, true),
      StructField("Staff", StringType, true),
      StructField("file_name", StringType, false)
    )
  )

  val expectedWithPartitionSchema = StructType(
    List(
      StructField("Day", DoubleType, true),
      StructField("Month", DoubleType, true),
      StructField("Customer ID", StringType, true),
      StructField("Customer Name", StringType, true),
      StructField("Standard Package", DoubleType, true),
      StructField("Extra Option 1", DoubleType, true),
      StructField("Extra Option 2", DoubleType, true),
      StructField("Extra Option 3", DoubleType, true),
      StructField("Staff", StringType, true),
      StructField("Quarter", IntegerType, true)
    )
  )
}

class GlobPartitionAndFileNameSuite extends FunSuite with DataFrameSuiteBase {
  import GlobPartitionAndFileNameSuite._

  private val dataRoot =
    getClass.getResource("/spreadsheets").getPath

  def readFromResources(
      path: String,
      inferSchema: Boolean
  ): DataFrame = spark.read
    .format("excel")
    .option("header", true)
    .option("inferSchema", inferSchema)
    .load(path)

  test("read multiple files must infer correct schema with inferSchema=true") {
    val df =
      readFromResources(s"$dataRoot/ca_dataset/2019/Quarter=4/*.xlsx", true)

    assert(df.schema == expectedInferredSchema)
  }

  test("read multiple files with input_file_name") {
    val df =
      readFromResources(s"$dataRoot/ca_dataset/2019/Quarter=4/*.xlsx", true)
        .withColumn("file_name", input_file_name)

    assert(df.schema == expectedWithFilenameSchema)

    /* And validate list of filename*/
    val names = df
      .select("file_name")
      .distinct
      .collect
      .map(r => r.getString(0))
      .map(p => Paths.get(p).getFileName.toString)
      .toSet

    assert(names == Set[String]("ca_10.xlsx", "ca_11.xlsx", "ca_12.xlsx"))
  }

  test("read whole folder with partition") {
    val df =
      readFromResources(s"$dataRoot/ca_dataset/2019", true)
    assert(df.schema == expectedWithPartitionSchema)

    /* And validate list of Quarters*/
    val quarters = df
      .select("Quarter")
      .distinct
      .collect
      .map(r => r.getInt(0))
      .toSet

    assert(quarters == Set[Int](1, 2, 3, 4))
  }

  test("read multiple files must has same number total number of rows") {
    val q4_total =
      readFromResources(s"$dataRoot/ca_dataset/2019/Quarter=4/*.xlsx", true)
        .count()

    val q4_sum = Seq("ca_10.xlsx", "ca_11.xlsx", "ca_12.xlsx")
      .map(name =>
        readFromResources(s"$dataRoot/ca_dataset/2019/Quarter=4/$name", true)
          .count()
      )
      .sum

    assert(q4_total > 0)
    assert(q4_total == q4_sum)
  }

}
