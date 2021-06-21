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
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

import scala.collection.JavaConverters._

object EncryptedReadSuite {
  val simpleSchema = StructType(List(
    StructField("A", DoubleType, true),
    StructField("B", DoubleType, true),
    StructField("C", DoubleType, true),
    StructField("D", DoubleType, true)
  ))

  val expectedData = List(Row(1.0d, 2.0d, 3.0d, 4.0d)).asJava
}

class EncryptedReadSuite extends FunSuite with DataFrameSuiteBase {
  import EncryptedReadSuite._

  lazy val expected = spark.createDataFrame(expectedData, simpleSchema)

  def readFromResources(path: String, password: String): DataFrame = {
    val url = getClass.getResource(path)
    spark.read.format("excel").option("dataAddress", "Sheet1!A1")
      .option("treatEmptyValuesAsNulls", true).option("workbookPassword", password)
      .option("inferSchema", true).load(url.getPath)
  }

  test("should read encrypted xslx file") {
    val df = readFromResources("/spreadsheets/simple_encrypted.xlsx", "fooba")

    assertDataFrameEquals(expected, df)
  }

  test("should read encrypted xls file") {
    val df = readFromResources("/spreadsheets/simple_encrypted.xls", "fooba")

    assertDataFrameEquals(expected, df)
  }
}
