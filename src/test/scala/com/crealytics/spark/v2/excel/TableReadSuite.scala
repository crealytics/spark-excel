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

/** Loading data from named table
  */
object TableReadSuite {
  val expectedSchema = StructType(List(
    StructField("City", StringType, true),
    StructField("Latitude", DoubleType, true),
    StructField("Longitude", DoubleType, true),
    StructField("Population", IntegerType, true)
  ))

  val expectedBigCitiesData: util.List[Row] = List(
    Row("Shanghai, China", 31.23d, 121.5d, 24256800),
    Row("Karachi, Pakistan", 24.86d, 67.01d, 23500000),
    Row("Beijing, China", 39.93d, 116.4d, 21516000),
    Row("São Paulo, Brazil", -23.53d, -46.63d, 21292900),
    Row("Delhi, India", 28.67d, 77.21d, 16788000),
    Row("Lagos, Nigeria", 6.45d, 3.47d, 16060000),
    Row("Istanbul, Turkey", 41.1d, 29d, 14657000),
    Row("Tokyo, Japan", 35.67d, 139.8d, 13298000),
    Row("Mumbai, India", 18.96d, 72.82d, 12478000),
    Row("Moscow, Russia", 55.75d, 37.62d, 12198000),
    Row("Guangzhou, China", 23.12d, 113.3d, 12081000),
    Row("Shenzhen, China", 22.55d, 114.1d, 10780000),
    Row("Total", 25.3966666666667d, 70.4666666666667d, 198905700)
  ).asJava

  val expectedSmallCitiesData: util.List[Row] = List(
    Row("Pyongyang, North Korea", 39.02d, 125.74d, 2581000),
    Row("Buenos Aires, Argentina", -34.6d, -58.38d, 2890000),
    Row("Seattle, Washington, USA", 47.61d, -122.33d, 609000),
    Row("Toronto, Canada", 43.7d, -79.4d, 2615000),
    Row("Auckland, New Zealand", -36.84d, 174.74d, 1454000),
    Row("Miami, Florida, USA", 25.78d, -80.21d, 400000),
    Row("Havana, Cuba", 23.13d, -82.38d, 2106000),
    Row("Fairbanks, Alaska, USA", 64.84d, -147.72d, 32000),
    Row("Longyearbyen, Svalbard", 78.22d, 15.55d, 2600),
    Row("Johannesburg, South Africa", -26.2d, 28d, 957000),
    Row("Cancun, Mexico", 21.16d, -86.85d, 722800),
    Row("Oahu, Hawaii, USA", 21.47d, -157.98d, 953000),
    Row("Total", 22.2741666666667d, -39.2683333333333d, 15322400)
  ).asJava

}

class TableReadSuite extends FunSuite with DataFrameSuiteBase {
  import TableReadSuite._

  def readFromResources(path: String, tableName: String, inferSchema: Boolean): DataFrame = {
    val url = getClass.getResource(path)
    spark.read.format("excel").option("dataAddress", s"$tableName[All]")
      .option("inferSchema", inferSchema).load(url.getPath)
  }

  test("should read named-table SmallCity with testing data from Apache POI upstream tests") {
    val df = readFromResources("/spreadsheets/apache_poi/DataTableCities.xlsx", "SmallCity", true)
    val expected = spark.createDataFrame(expectedSmallCitiesData, expectedSchema)
    assertDataFrameApproximateEquals(expected, df, 0.1e-3)
  }

  test("should read named-table BigCity with testing data from Apache POI upstream tests") {
    val df = readFromResources("/spreadsheets/apache_poi/DataTableCities.xlsx", "BigCity", true)
    val expected = spark.createDataFrame(expectedBigCitiesData, expectedSchema)
    assertDataFrameApproximateEquals(expected, df, 0.1e-3)
  }
}
