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

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec

import java.io.File

class MassivePartitionReadSuite
    extends AnyWordSpec
    with DataFrameSuiteBase
    with LocalFileTestingUtilities
    with BeforeAndAfterAll {

  /** Checks that the excel data files in given folder equal the provided dataframe */
  private def assertWrittenExcelData(expectedDf: DataFrame, folder: String): Unit = {
    val actualDf = spark.read
      .format("excel")
      .option("path", folder)
      .load()

    /* assertDataFrameNoOrderEquals is sensitive to order of columns, so we
      order both dataframes in the same way
     */
    val orderedSchemaColumns = expectedDf.schema.fields.map(f => f.name).sorted

    assertDataFrameNoOrderEquals(
      expectedDf.select(orderedSchemaColumns.head, orderedSchemaColumns.tail: _*),
      actualDf.select(orderedSchemaColumns.head, orderedSchemaColumns.tail: _*)
    )

  }

  var targetDir: File = null
  var expectedDf: DataFrame = null

  def createExpected(): Unit = {
    targetDir = createTemporaryDirectory("v2")

    val dfCsv = spark.read
      .format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      .option("path", "src/test/resources/v2readwritetest/partition_csv/partition.csv")
      .load()

    val dfFinal = dfCsv.unionAll(dfCsv)

    val dfWriter = dfFinal.write
      .partitionBy("col1")
      .format("excel")
      .option("path", targetDir.getPath)
      .option("header", value = true)
      .mode(SaveMode.Append)

    dfWriter.save()
    dfWriter.save()

    val orderedSchemaColumns = dfCsv.schema.fields.map(f => f.name).sorted
    expectedDf = dfFinal
      .unionAll(dfFinal)
      .withColumn("col1", col("col1").cast(IntegerType))
      .select(orderedSchemaColumns.head, orderedSchemaColumns.tail: _*)
  }

  // Delete the temp file
  override def afterAll(): Unit = {
    deleteDirectory(targetDir)
  }

  for (run <- Range(0, 5)) {

    s"many partitions read (run=$run)" in {
      assume(spark.sparkContext.version >= "3.0.1")
      if (run == 0) {
        createExpected()
      }
      assertWrittenExcelData(expectedDf, targetDir.getPath)
    }
  }

}
