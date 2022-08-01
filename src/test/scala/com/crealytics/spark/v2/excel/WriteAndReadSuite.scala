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

import org.apache.spark.sql.Row
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

import java.util
import scala.jdk.CollectionConverters._
import java.nio.file.Files
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, ZoneId}
import java.time.format.DateTimeFormatter

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
      StructField("Id", IntegerType, nullable = true),
      StructField("Date", DateType, nullable = true),
      StructField("Timestamp", TimestampType, nullable = true)
    )
  )

  val expectedData_02: List[Row] = List(
    Row(1, "2021-10-01", "2021-10-01 01:23:45"),
    Row(2, "2021-11-01", "2021-11-01 11:23:45"),
    Row(3, "2021-10-11", "2021-10-11 01:23:45"),
    Row(4, "2021-11-11", "2021-11-11 01:23:05"),
    Row(5, "2022-10-01", "2022-10-01 16:23:45")
  )

  val expectedData_bool: List[Row] = List(Row(1, true), Row(2, false))

  val userDefinedSchema_bool = StructType(
    List(StructField("Id", IntegerType, nullable = true), StructField("Bool", BooleanType, nullable = true))
  )
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

  test("write then read java.sql.Date and java.sql.Timestamp") {
    val path = Files.createTempDirectory("spark_excel_wr_02_").toString()
    val previousConfigValue = spark.conf.getOption(DATETIME_JAVA8API_ENABLED)
    spark.conf.set(DATETIME_JAVA8API_ENABLED, false)
    val expectedData_02_sql = expectedData_02
      .map(r => Row.fromTuple((r.getInt(0), Date.valueOf(r.getString(1)), Timestamp.valueOf(r.getString(2)))))
      .asJava
    val df_source = spark.createDataFrame(expectedData_02_sql, userDefinedSchema_02).sort("Id")
    df_source.write.format("excel").mode(SaveMode.Append).save(path)

    val df_read = spark.read
      .format("excel")
      .schema(userDefinedSchema_02)
      .load(path)
      .sort("Id")

    assertDataFrameEquals(df_source, df_read)

    /* Cleanup, should after the checking */
    if (previousConfigValue.isEmpty) {
      spark.conf.unset(DATETIME_JAVA8API_ENABLED)
    } else {
      spark.conf.set(DATETIME_JAVA8API_ENABLED, previousConfigValue.get)
    }
    deleteDirectory(path)
  }

  test("write then read java.time.Instant and java.time.LocalDate") {
    if (spark.version.startsWith("2.")) {
      cancel(DATETIME_JAVA8API_ENABLED + " didn't exist before spark 3.0. Nothing to test!")
    }
    val path = Files.createTempDirectory("spark_excel_wr_02_").toString()
    val previousConfigValue = spark.conf.get(DATETIME_JAVA8API_ENABLED)
    spark.conf.set(DATETIME_JAVA8API_ENABLED, true)
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault)
    val expectedData_02_time = expectedData_02
      .map(r =>
        Row.fromTuple((r.getInt(0), LocalDate.parse(r.getString(1)), Instant.from(formatter.parse(r.getString(2)))))
      )
      .asJava
    val df_source = spark.createDataFrame(expectedData_02_time, userDefinedSchema_02).sort("Id")
    df_source.write.format("excel").mode(SaveMode.Append).save(path)

    val df_read = spark.read
      .format("excel")
      .schema(userDefinedSchema_02)
      .load(path)
      .sort("Id")

    assertDataFrameEquals(df_source, df_read)

    /* Cleanup, should after the checking */
    spark.conf.set(DATETIME_JAVA8API_ENABLED, previousConfigValue)
    deleteDirectory(path)
  }

  test("write then read boolean") {
    val path = Files.createTempDirectory("spark_excel_wr_bool_").toString()
    val previousConfigValue = spark.conf.getOption(DATETIME_JAVA8API_ENABLED)
    spark.conf.set(DATETIME_JAVA8API_ENABLED, false)
    val expectedData_bool_sql = expectedData_bool.asJava
    val df_source = spark.createDataFrame(expectedData_bool_sql, userDefinedSchema_bool).sort("Id")
    df_source.write.format("excel").mode(SaveMode.Append).save(path)

    val df_read = spark.read
      .format("excel")
      .schema(userDefinedSchema_bool)
      .load(path)
      .sort("Id")

    assertDataFrameEquals(df_source, df_read)

    /* Cleanup, should after the checking */
    if (previousConfigValue.isEmpty) {
      spark.conf.unset(DATETIME_JAVA8API_ENABLED)
    } else {
      spark.conf.set(DATETIME_JAVA8API_ENABLED, previousConfigValue.get)
    }
    deleteDirectory(path)
  }

}
