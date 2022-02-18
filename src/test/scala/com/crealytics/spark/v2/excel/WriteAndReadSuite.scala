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

import org.apache.spark.sql.Row
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

import java.util
import scala.collection.JavaConverters._
import java.nio.file.Files
import java.sql.{Date, Timestamp}

import com.holdenkarau.spark.testing.DataFrameSuiteBase

/** Writing and reading back */
object WriteAndReadSuite {

  val DATETIME_JAVA8API_ENABLED = "spark.sql.datetime.java8API.enabled"

  val userDefinedSchema_01 = StructType(
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

  val expectedData_01: util.List[Row] = List(
    Row(1, 12, "CA869", "Phạm Uyển Trinh", null, null, 2200, null, "Ella Fitzgerald"),
    Row(1, 12, "CA870", "Nguyễn Liên Thảo", null, null, 2000, 1350, "Ella Fitzgerald"),
    Row(1, 12, "CA871", "Lê Thị Nga", 17000, null, null, null, "Ella Fitzgerald"),
    Row(1, 12, "CA872", "Phan Tố Nga", null, null, 2000, null, "Teresa Teng"),
    Row(1, 12, "CA873", "Nguyễn Thị Teresa Teng", null, null, 1200, null, "Jesse Thomas")
  ).asJava

  val userDefinedSchema_02 = StructType(
    List(
      StructField("Day", LongType, true),
      StructField("Month", LongType, true),
      StructField("Customer ID", StringType, true),
      StructField("Customer Name", StringType, true),
      StructField("Standard Package", IntegerType, true),
      StructField("Extra Option 1", IntegerType, true),
      StructField("Extra Option 2", IntegerType, true),
      StructField("Extra Option 3", LongType, true),
      StructField("Staff", StringType, true)
    )
  )

  val expectedData_02: util.List[Row] = List(
    Row(1L, 12L, "CA869", "Phạm Uyển Trinh", null, null, 2200, null, "Ella Fitzgerald"),
    Row(1L, 12L, "CA870", "Nguyễn Liên Thảo", null, null, 2000, 1350L, "Ella Fitzgerald"),
    Row(1L, 12L, "CA871", "Lê Thị Nga", 17000, null, null, null, "Ella Fitzgerald"),
    Row(1L, 12L, "CA872", "Phan Tố Nga", null, null, 2000, null, "Teresa Teng"),
    Row(1L, 12L, "CA873", "Nguyễn Thị Teresa Teng", null, null, 1200, null, "Jesse Thomas")
  ).asJava

  val userDefinedSchema_03 = StructType(
    List(
      StructField("Id", IntegerType, nullable = true),
      StructField("Date", DateType, nullable = true),
      StructField("Timestamp", TimestampType, nullable = true)
    )
  )

  val expectedData_03: util.List[Row] = List(
    Row(1, Date.valueOf("2021-10-01"), Timestamp.valueOf("2021-10-01 01:23:45")),
    Row(2, Date.valueOf("2021-11-01"), Timestamp.valueOf("2021-11-01 11:23:45")),
    Row(3, Date.valueOf("2021-10-11"), Timestamp.valueOf("2021-10-11 01:23:45")),
    Row(4, Date.valueOf("2021-11-11"), Timestamp.valueOf("2021-11-11 01:23:05")),
    Row(5, Date.valueOf("2022-10-01"), Timestamp.valueOf("2022-10-01 16:23:45"))
  ).asJava
}

/** Write then read excel file, with both XLSX and XLS formats. There are two open questions:
  *
  * 1, Write to existing files: (multiple RDD partitions), the logic of how to write to existing files (multiple) is
  * still an open question 2, Write to named table (XLSX): How to address that table? And the same issue with existing
  * tables on multiple files
  *
  * There are two approach that we can think of: 1, User provide excel file template, will work for both cases 2, For
  * (2,) still create a table, with extra "tableName" option along side with dataAddress option.
  */
class WriteAndReadSuite extends AnyFunSuite with DataFrameSuiteBase with ExcelTestingUtilities {
  import WriteAndReadSuite._

  test("simple write then read") {
    val path = Files.createTempDirectory("spark_excel_wr_01_").toString()
    val df_source = spark.createDataFrame(expectedData_01, userDefinedSchema_01).sort("Customer ID")
    df_source.write.format("excel").mode(SaveMode.Append).save(path)

    val df_read = spark.read
      .format("excel")
      .schema(userDefinedSchema_01)
      .load(path)
      .sort("Customer ID")

    assertDataFrameEquals(df_source, df_read)

    /* Cleanup, should after the checking */
    deleteDirectory(path)
  }

  test("write and read with difference addresses") {

    Seq("A1", "B4", "X15", "AB8", "Customer!AB8", "'Product Values'!C5").foreach(dataAddress => {
      val path = Files.createTempDirectory("spark_excel_wr_02_").toString()
      val df_source = spark
        .createDataFrame(expectedData_01, userDefinedSchema_01)
        .sort("Customer ID")

      df_source.write
        .format("excel")
        .option("dataAddress", dataAddress)
        .mode(SaveMode.Append)
        .save(path)

      val df_read = spark.read
        .format("excel")
        .option("dataAddress", dataAddress)
        .schema(userDefinedSchema_01)
        .load(path)
        .sort("Customer ID")

      assertDataFrameEquals(df_source, df_read)

      /* Cleanup, should after the checking */
      deleteDirectory(path)
    })
  }

  test("write then read java.sql.date and java.sql.timestamp") {
    val path = Files.createTempDirectory("spark_excel_wr_03_").toString()
    spark.conf.set(DATETIME_JAVA8API_ENABLED, false)
    val df_source = spark.createDataFrame(expectedData_03, userDefinedSchema_03).sort("Id")
    df_source.write.format("excel").mode(SaveMode.Append).save(path)

    val df_read = spark.read
      .format("excel")
      .schema(userDefinedSchema_03)
      .load(path)
      .sort("Id")

    assertDataFrameEquals(df_source, df_read)

    /* Cleanup, should after the checking */
    spark.conf.unset(DATETIME_JAVA8API_ENABLED)
    deleteDirectory(path)
  }

  test("write then read java.time.Instant and java.time.LocalDate") {
    val path = Files.createTempDirectory("spark_excel_wr_03_").toString()
    spark.conf.set(DATETIME_JAVA8API_ENABLED, true)
    val df_source = spark.createDataFrame(expectedData_03, userDefinedSchema_03).sort("Id")
    df_source.write.format("excel").mode(SaveMode.Append).save(path)

    val df_read = spark.read
      .format("excel")
      .schema(userDefinedSchema_03)
      .load(path)
      .sort("Id")

    assertDataFrameEquals(df_source, df_read)

    /* Cleanup, should after the checking */
    spark.conf.unset(DATETIME_JAVA8API_ENABLED)
    deleteDirectory(path)
  }

}
