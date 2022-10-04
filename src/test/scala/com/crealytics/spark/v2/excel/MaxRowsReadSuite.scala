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

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class MaxRowsReadSuite extends AnyWordSpec with DataFrameSuiteBase with Matchers {

  "excel v2 and maxNumRows" can {

    case class Testcase(hasHeader: Boolean, doInferSchema: Boolean, numRows: Int, maxRowsInMemory: Int)

    val allTestcases = Seq(
      Testcase(hasHeader = false, doInferSchema = false, numRows = 2242, maxRowsInMemory = 200),
      Testcase(hasHeader = true, doInferSchema = false, numRows = 2241, maxRowsInMemory = 200),
      Testcase(hasHeader = true, doInferSchema = true, numRows = 2241, maxRowsInMemory = 200)
    )

    for (testcase <- allTestcases) {
      s"v2 streaming read (header = ${testcase.hasHeader}, inferSchema = ${testcase.doInferSchema}, " +
        s"maxRowsInMemory = ${testcase.maxRowsInMemory})" in {

          val dfExcel = spark.read
            .format("excel")
            .option("path", "src/test/resources/v2readwritetest/large_excel/largefile-wide-single-sheet.xlsx")
            .option("header", value = testcase.hasHeader)
            .option("maxRowsInMemory", testcase.maxRowsInMemory.toString)
            .option("inferSchema", testcase.doInferSchema)
            .load()

          dfExcel.count() shouldEqual testcase.numRows

        }
    }
  }
}
