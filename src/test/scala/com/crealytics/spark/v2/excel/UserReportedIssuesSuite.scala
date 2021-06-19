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
import org.apache.spark.sql.Row
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

import java.util
import scala.collection.JavaConverters._

object UserReportedIssuesSuite {

  /* Issue: https://github.com/crealytics/spark-excel/issues/285*/
  val expectedSchema_Issue285 = StructType(
    List(
      StructField("1", StringType, true),
      StructField("2", StringType, true),
      StructField("3", StringType, true)
    )
  )

  /** No change to the spark-excel, Apache POI also produce same result with
    * sheet.iterator
    *
    * Workaround:
    *   https://stackoverflow.com/questions/47790569/how-to-avoid-skipping-blank-rows-or-columns-in-apache-poi
    *   Doc: http://poi.apache.org/components/spreadsheet/quick-guide.html#Iterator
    */
  val expectedData_Issue285: util.List[Row] =
    List(
      Row("File info", null, null),
      Row("Info", "Info", "Info"),
      Row("Metadata", null, null),
      Row(null, "1", "2"),
      Row("A", "1", "2"),
      Row("B", "5", "6"),
      Row("C", "9", "10"),
      Row("Metadata", null, null),
      Row(null, "1", "2"),
      Row("A", "1", "2"),
      Row("B", "4", "5"),
      Row("C", "7", "8")
    ).asJava

  /* With newly introduced keepUndefinedRows option*/
  val expectedData_KeepUndefinedRows_Issue285: util.List[Row] =
    List(
      Row("File info", null, null),
      Row("Info", "Info", "Info"),
      Row(null, null, null),
      Row("Metadata", null, null),
      Row(null, null, null),
      Row(null, "1", "2"),
      Row("A", "1", "2"),
      Row("B", "5", "6"),
      Row("C", "9", "10"),
      Row(null, null, null),
      Row(null, null, null),
      Row("Metadata", null, null),
      Row(null, null, null),
      Row(null, "1", "2"),
      Row("A", "1", "2"),
      Row("B", "4", "5"),
      Row("C", "7", "8")
    ).asJava
}

class UserReportedIssuesSuite extends FunSuite with DataFrameSuiteBase {
  import UserReportedIssuesSuite._

  def readFromResources(
      path: String,
      keepUndefinedRows: Boolean,
      inferSchema: Boolean
  ): DataFrame = {
    val url = getClass.getResource(path)
    if (inferSchema)
      spark.read
        .format("excel")
        .option("header", false)
        .option("inferSchema", true)
        .option("keepUndefinedRows", keepUndefinedRows)
        .load(url.getPath)
    else
      spark.read
        .format("excel")
        .option("header", false)
        .option("inferSchema", true)
        .option("keepUndefinedRows", keepUndefinedRows)
        .schema(expectedSchema_Issue285)
        .load(url.getPath)
  }

  test("#285 undefined rows: no keep") {
    val df =
      readFromResources("/spreadsheets/issue_285_bryce21.xlsx", false, false)
    val expected =
      spark.createDataFrame(expectedData_Issue285, expectedSchema_Issue285)
    assertDataFrameEquals(expected, df)
  }

  test("#285 undefined rows: keep") {
    val df =
      readFromResources("/spreadsheets/issue_285_bryce21.xlsx", true, false)
    val expected =
      spark.createDataFrame(
        expectedData_KeepUndefinedRows_Issue285,
        expectedSchema_Issue285
      )
    assertDataFrameEquals(expected, df)
  }

}
