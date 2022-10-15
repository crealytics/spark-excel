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

package com.crealytics.spark.v2.excel

import com.crealytics.spark.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

import java.util
import scala.jdk.CollectionConverters._
import java.sql.Date

object UserReportedIssuesSuite {

  /** Issue: https://github.com/crealytics/spark-excel/issues/463 Cannot load Date and Decimal fields
    */
  val userDefined_Issue463 = StructType(
    List(
      StructField("itm no", StringType, true),
      StructField("Expense", DecimalType(23, 10), true),
      StructField("Date", DateType, true)
    )
  )

  val expectedData_Issue463: util.List[Row] =
    List(Row("item1", Decimal("1.1"), Date.valueOf("2021-10-01"))).asJava

}

class UserReportedIssuesSuite extends AnyFunSuite with DataFrameSuiteBase with ExcelTestingUtilities {
  import UserReportedIssuesSuite._

  test("#463 Date and decimal with user defined schema") {
    val df = readFromResources(
      spark,
      path = "issue_463_cristichircu.xlsx",
      options = Map("header" -> true),
      schema = userDefined_Issue463
    )
    val expected = spark.createDataFrame(expectedData_Issue463, userDefined_Issue463)
    assertDataFrameEquals(expected, df)
  }
}
