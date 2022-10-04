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

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class MaxRowsReadSuite extends AnyWordSpec with DataFrameSuiteBase with Matchers {

  "excel v1 and maxNumRows" can {

    "read with maxNumRows=200" in {

      val dfExcel = spark.read
        .format("com.crealytics.spark.excel")
        .option("path", "src/test/resources/v2readwritetest/large_excel/largefile-wide-single-sheet.xlsx")
        .option("header", value = true)
        .option("maxRowsInMemory", "200")
        .option("inferSchema", false)
        .load()

      dfExcel.count() shouldEqual 2241
    }

  }
}
