/*
 * Copyright 2023 Martin Mauch (@nightscape)
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

package com.crealytics.spark.excel.v2

import com.crealytics.spark.DataFrameSuiteBase
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

/** Issue References:
  *
  * #52. input_file_name returns empty string https://github.com/crealytics/spark-excel/issues/52
  *
  * #74. Allow reading multiple files specified as a list OR by a pattern
  * https://github.com/crealytics/spark-excel/issues/74
  *
  * #97. Reading multiple files https://github.com/crealytics/spark-excel/issues/97
  */

object GlobPartitionAndFileNameSuite {
  val expectedInferredSchema = StructType(
    List(
      StructField("Day", IntegerType, true),
      StructField("Month", IntegerType, true),
      StructField("Customer ID", StringType, true),
      StructField("Customer Name", StringType, true),
      StructField("Standard Package", IntegerType, true),
      StructField("Extra Option 1", IntegerType, true),
      StructField("Extra Option 2", IntegerType, true),
      StructField("Extra Option 3", IntegerType, true),
      StructField("Staff", StringType, true)
    )
  )

  val expectedWithFilenameSchema = StructType(
    List(
      StructField("Day", IntegerType, true),
      StructField("Month", IntegerType, true),
      StructField("Customer ID", StringType, true),
      StructField("Customer Name", StringType, true),
      StructField("Standard Package", IntegerType, true),
      StructField("Extra Option 1", IntegerType, true),
      StructField("Extra Option 2", IntegerType, true),
      StructField("Extra Option 3", IntegerType, true),
      StructField("Staff", StringType, true),
      StructField("file_name", StringType, false)
    )
  )

  val expectedWithPartitionSchema = StructType(
    List(
      StructField("Day", IntegerType, true),
      StructField("Month", IntegerType, true),
      StructField("Customer ID", StringType, true),
      StructField("Customer Name", StringType, true),
      StructField("Standard Package", IntegerType, true),
      StructField("Extra Option 1", IntegerType, true),
      StructField("Extra Option 2", IntegerType, true),
      StructField("Extra Option 3", IntegerType, true),
      StructField("Staff", StringType, true),
      StructField("Quarter", IntegerType, true)
    )
  )
}

class GlobPartitionAndFileNameSuite extends AnyFunSuite with DataFrameSuiteBase with ExcelTestingUtilities {
  import GlobPartitionAndFileNameSuite._

  private val sharedOptions = Map("header" -> true, "inferSchema" -> true)

  test("read multiple files must infer correct schema with inferSchema=true") {
    val df = readFromResources(spark, "ca_dataset/2019/Quarter=4/*.xlsx", sharedOptions)
    assert(df.schema == expectedInferredSchema)
  }

  test("read multiple files with input_file_name") {
    val df = readFromResources(spark, "ca_dataset/2019/Quarter=4/*.xlsx", sharedOptions)
      .withColumn("file_name", input_file_name())
    assert(df.schema == expectedWithFilenameSchema)

    /* And validate list of filename */
    val names = df
      .select("file_name")
      .distinct()
      .collect()
      .map(r => r.getString(0))
      .map(p => p.split("[\\/]").last) // this works on Windows too
      .toSet
    assert(names == Set[String]("ca_10.xlsx", "ca_11.xlsx", "ca_12.xlsx"))
  }

  test("read whole folder with partition") {
    val df = readFromResources(spark, "ca_dataset/2019", sharedOptions)
    assert(df.schema == expectedWithPartitionSchema)

    /* And validate list of Quarters */
    val quarters = df.select("Quarter").distinct().collect().map(r => r.getInt(0)).toSet
    assert(quarters == Set[Int](1, 2, 3, 4))
  }

  test("read multiple files must has same number total number of rows") {
    val q4_total = readFromResources(spark, "ca_dataset/2019/Quarter=4/*.xlsx", sharedOptions)
      .count()

    val q4_sum = Seq("ca_10.xlsx", "ca_11.xlsx", "ca_12.xlsx")
      .map(name => readFromResources(spark, s"ca_dataset/2019/Quarter=4/$name", sharedOptions).count())
      .sum

    assert(q4_total > 0)
    assert(q4_total == q4_sum)
  }

}
