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

import cats.Monoid
import com.crealytics.spark.DataFrameSuiteBase
import org.apache.poi.ss.util.CellReference
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.scalacheck.Shrink
import org.scalactic.anyvals.PosInt
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import spoiwo.model.{Cell, CellRange, Row => SRow, Sheet, Table => STable}
import spoiwo.natures.xlsx.Model2XlsxConversions._

import java.io.{File, FileOutputStream}
import scala.collection.compat._

abstract class IntegrationSuite(implementation: String)
    extends AnyFunSpec
    with ScalaCheckPropertyChecks
    with DataFrameSuiteBase
    with Matchers
    with Generators {
  import spark.implicits._

  implicit def shrinkOnlyNumberOfRows[A]: Shrink[List[A]] = Shrink.shrinkContainer[List, A]
  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = PosInt.from(sys.env.getOrElse("EXAMPLES_PER_PROPERTY", "6").toInt).get)

  // inferring the schema will not match the original types exactly
  def inferredDataTypes(schema: StructType): Seq[Function[Seq[Any], DataType]] =
    schema.map(_.dataType).map { dt =>
      val pf: Function[Seq[Any], DataType] = dt match {
        case _: DecimalType => {
          case values: Seq[Any] if values.distinct == Seq("") => StringType
          case _ => DoubleType
        }
        case _: NumericType =>
          _: Seq[Any] => DoubleType
        case DateType =>
          _: Seq[Any] => TimestampType
        case t: DataType =>
          _: Seq[Any] => t
      }
      pf
    }

  def expectedDataTypes(inferred: DataFrame): Seq[(String, DataType)] = {
    val data = inferred.collect()
    inferredDataTypes(exampleDataSchema)
      .to(List)
      .zip(inferred.schema)
      .zipWithIndex
      .map { case ((f, sf), idx) => sf.name -> f(data.toIndexedSeq.map(_.get(idx))) }
  }

  def runTests(maxRowsInMemory: Option[Int], maxByteArraySize: Option[Int] = None): Unit = {
    def writeThenRead(
      df: DataFrame,
      schema: Option[StructType] = Some(exampleDataSchema),
      fileName: Option[String] = None,
      saveMode: SaveMode = SaveMode.Overwrite,
      dataAddress: Option[String] = None,
      header: Boolean = true
    ): DataFrame = {
      val theFileName = fileName.getOrElse(File.createTempFile("spark_excel_test_", ".xlsx").getAbsolutePath)

      val writer = df.write
        .format(implementation)
        .option("dataAddress", s"'$sheetName'!A1")
        .option("header", header)
        .mode(saveMode)
      val configuredWriter =
        Map("dataAddress" -> dataAddress).foldLeft(writer) {
          case (wri, (key, Some(value))) => wri.option(key, value)
          case (wri, _) => wri
        }
      configuredWriter.save(theFileName)

      val reader = spark.read.format(implementation).option("dataAddress", s"'$sheetName'!A1").option("header", header)
      val configuredReader = Map(
        "maxRowsInMemory" -> maxRowsInMemory,
        "maxByteArraySize" -> maxByteArraySize,
        "inferSchema" -> Some(schema.isEmpty),
        "excerptSize" -> Some(10),
        "dataAddress" -> dataAddress
      ).foldLeft(reader) {
        case (rdr, (key, Some(value))) => rdr.option(key, value.toString)
        case (rdr, _) => rdr
      }
      val withSchema = schema.foldLeft(configuredReader)(_ schema _)
      withSchema.load(theFileName)
    }

    def assertEqualAfterInferringTypes(original: DataFrame, inferred: DataFrame): Unit = {
      val originalWithInferredColumnTypes =
        original.schema
          .zip(expectedDataTypes(inferred).map(_._2))
          .foldLeft(original) { case (df, (field, dataType)) =>
            df.withColumn(field.name, df(field.name).cast(dataType))
          }
      val expected = spark.createDataFrame(originalWithInferredColumnTypes.rdd, inferred.schema)
      assertDataFrameEquals(expected, inferred)
    }

    describe(
      s"with implementation = $implementation, maxRowsInMemory = $maxRowsInMemory; maxByteArraySize = $maxByteArraySize"
    ) {
      it("parses known datatypes correctly") {
        forAll(rowsGen) { rows =>
          val expected = spark.createDataset(rows).toDF()
          val actual = writeThenRead(expected)
          assertDataFrameApproximateEquals(expected, actual, relTol = 1.0e-6)
        }
      }

      it("reads blank cells as null and empty string cells as \"\"") {
        forAll(rowsGen) { rows =>
          val expected = spark.createDataset(rows).toDF()

          // We need two dataframes, one with null values, one with empty strings.
          // This is because we want ExcelFileSaver to write an empty string
          // if there's a null in that column.
          // expectedWithEmptyStr is what the dataframe should look
          // like when the Excel spreadsheet is saved.
          // TODO: The following variable should actually be used for the tests
          // val expectedWithNull = expected.withColumn("aString", lit(null: String))
          // Generate the same DataFrame but with empty strings
          val expectedWithEmptyStr = expected.withColumn("aString", lit("": String))
          // Set the schema so that aString is nullable
          val fields = expectedWithEmptyStr.schema.fields
          fields.update(fields.indexWhere(_.name == "aString"), StructField("aString", DataTypes.StringType, true))

          assertDataFrameApproximateEquals(expectedWithEmptyStr, writeThenRead(expectedWithEmptyStr), relTol = 1.0e-6)
        }
      }

      it("infers schema correctly") {
        forAll(rowsGen) { rows =>
          val df = spark.createDataset(rows).toDF()
          val inferred = writeThenRead(df, schema = None)

          val nonNullCounts: Array[Map[String, Int]] =
            df.collect().map(r => df.schema.map(f => f.name -> (if (r.getAs[Any](f.name) != null) 1 else 0)).toMap)
          val (inferableColumns, nonInferableColumns) = Monoid.combineAll(nonNullCounts).partition(_._2 > 0)
          // Without actual data, we assume everything is a StringType
          nonInferableColumns.keys.foreach(k => assert(inferred.schema(k).dataType == StringType))
          val expectedTypeMap = expectedDataTypes(inferred).toMap
          val (actualTypes, expTypes) =
            inferableColumns.keys
              .map(k => (inferred.schema(k).dataType, expectedTypeMap(k)))
              .unzip
          assert(actualTypes == expTypes)
        }
      }

      it("returns all data rows when inferring schema") {
        forAll(rowsGen.filter(_.nonEmpty)) { rows =>
          val original = spark.createDataset(rows).toDF()
          val inferred = writeThenRead(original, schema = None)
          assertEqualAfterInferringTypes(original, inferred)
        }
      }

      it("handles multi-line column headers correctly") {
        forAll(rowsGen.filter(_.nonEmpty)) { rows =>
          val original = spark.createDataset(rows).toDF()
          val multiLineHeaders = original.withColumnRenamed("aString", "a\nString")
          val inferred = writeThenRead(multiLineHeaders, schema = None)
          assertEqualAfterInferringTypes(multiLineHeaders, inferred)
        }
      }

      it("respects the given column names in a user-specified schema") {
        forAll(rowsGen.filter(_.nonEmpty)) { rows =>
          val renamedSchema = StructType(exampleDataSchema.fields.map(f => f.copy(name = s"${f.name}CustomName")))
          val original = spark.createDataset(rows).toDF()
          val expected = spark.createDataset(rows).toDF(renamedSchema.fieldNames.toIndexedSeq: _*)
          val inferred = writeThenRead(original, schema = Some(renamedSchema))
          assertDataFrameApproximateEquals(expected, inferred, relTol = 1.0e-6)
        }
      }

      it("reads files without headers correctly") {
        forAll(dataAndLocationGen.filter(_._1.nonEmpty)) { case (rows, startCellAddress, endCellAddress) =>
          val original = spark.createDataset(rows).toDF()
          val renamed = spark.createDataset(rows).toDF(original.schema.fieldNames.indices.map(i => s"_c$i"): _*)
          val fileName = File.createTempFile("spark_excel_test_", ".xlsx").getAbsolutePath
          val inferred = writeThenRead(
            original,
            schema = None,
            header = false,
            fileName = Some(fileName),
            dataAddress = Some(s"'$sheetName'!${startCellAddress.formatAsString()}:${endCellAddress.formatAsString()}")
          )
          assertEqualAfterInferringTypes(renamed, inferred)
        }
      }

      it("reads files with missing cells correctly") {
        forAll(rowsGen.filter(_.nonEmpty)) { _ =>
          val fileName = File.createTempFile("spark_excel_test_", ".xlsx").getAbsolutePath
          val numCols = 20
          /*
            Generate some header names.
            Some written headers are set to an empty String,
            but we also store the column names spark-excel should give them.
           */
          val (writtenHeaderNames, expectedHeaderNames) =
            (0 until numCols).map(c => if (c % 3 == 0) ("", s"_c$c") else (s"header_$c", s"header_$c")).unzip
          val existingData = Sheet(
            name = sheetName,
            rows = SRow(
              writtenHeaderNames.zipWithIndex.map { case (header, c) => Cell(header, index = c) },
              index = 0
            ) ::
              (0 until 100)
                .map(r => SRow((0 until numCols).filter(_ % 2 == 0).map(c => Cell(s"$r,$c", index = c)), index = r + 1))
                .to(List)
          )
          existingData.convertAsXlsx().write(new FileOutputStream(new File(fileName)))
          val allData = spark.read
            .excel(dataAddress = s"'$sheetName'!A1", inferSchema = true)
            .load(fileName)
          allData.schema.fieldNames should equal(expectedHeaderNames)
          val (headersWithData, headersWithoutData) = expectedHeaderNames.zipWithIndex.partition(_._2 % 2 == 0)
          val expectedContents = existingData.rows.drop(1).map(_.cells.map(_.value))
          val actualContents = allData.select(headersWithData.map(c => col(c._1)): _*).collect().map(_.toSeq)
          actualContents should contain theSameElementsInOrderAs expectedContents
          val emptyContents = allData.select(headersWithoutData.map(c => col(c._1)): _*).collect().map(_.toSeq)
          emptyContents should contain theSameElementsInOrderAs (0 until existingData.rows.size - 1)
            .map(_ => headersWithoutData.map(_ => null))
        }
      }

      def withFileOutputStream[T](fileName: String)(f: FileOutputStream => T): T = {
        val outputStream = new FileOutputStream(new File(fileName))
        val res = f(outputStream)
        outputStream.close()
        res
      }

      it("writes to and reads from the specified dataAddress, leaving non-overlapping existing data alone") {
        forAll(dataAndLocationGen.filter(_._1.nonEmpty), sheetGen) {
          case ((rows, startCellAddress, endCellAddress), existingData) =>
            val fileName = File.createTempFile("spark_excel_test_", ".xlsx").getAbsolutePath
            withFileOutputStream(fileName)(existingData.convertAsXlsx().write)
            val original = spark.createDataset(rows).toDF()
            val inferred =
              writeThenRead(
                original,
                schema = None,
                fileName = Some(fileName),
                saveMode = SaveMode.Append,
                dataAddress =
                  Some(s"'$sheetName'!${startCellAddress.formatAsString()}:${endCellAddress.formatAsString()}")
              )

            assertEqualAfterInferringTypes(original, inferred)

            assertNoDataOverwritten(existingData, fileName, startCellAddress, endCellAddress)
        }
      }

      if (maxRowsInMemory.isEmpty) {
        it("writes to and reads from the specified table, leaving non-overlapping existing data alone") {
          forAll(dataAndLocationGen.filter(_._1.nonEmpty), sheetGen) {
            case ((rows, startCellAddress, endCellAddress), sheet) =>
              val fileName = File.createTempFile("spark_excel_test_", ".xlsx").getAbsolutePath
              val tableName = "SomeTable"

              val existingData = sheet.withTables(
                STable(
                  cellRange = CellRange(
                    rowRange = (startCellAddress.getRow, endCellAddress.getRow),
                    columnRange = (startCellAddress.getCol.toInt, endCellAddress.getCol.toInt)
                  ),
                  name = tableName,
                  displayName = tableName
                )
              )
              val original = spark.createDataset(rows).toDF()
              withFileOutputStream(fileName)(existingData.convertAsXlsx().write)
              val inferred =
                writeThenRead(
                  original,
                  schema = None,
                  fileName = Some(fileName),
                  saveMode = SaveMode.Append,
                  dataAddress = Some(s"$tableName[#All]")
                )

              assertEqualAfterInferringTypes(original, inferred)

              assertNoDataOverwritten(existingData, fileName, startCellAddress, endCellAddress)
          }
        }
      }
    }
  }

  private def assertNoDataOverwritten(
    existingData: Sheet,
    fileName: String,
    startCellAddress: CellReference,
    endCellAddress: CellReference
  ): Unit = {
    val nonOverwrittenData = existingData.withRows(existingData.rows.map { row =>
      row.withCells(
        row.cells.filterNot(c =>
          c.index.get >= startCellAddress.getCol &&
            c.index.get <= endCellAddress.getCol &&
            row.index.get >= startCellAddress.getRow &&
            row.index.get <= endCellAddress.getRow
        )
      )
    })
    val allData = spark.read
      .excel(dataAddress = s"'$sheetName'!A1", header = false, inferSchema = false)
      .load(fileName)
      .collect()
      .map(_.toSeq)

    val differencesInNonOverwrittenData = nonOverwrittenData.rows.flatMap { row =>
      row.cells.flatMap { cell =>
        val actualData = for {
          row <- allData.lift(row.index.get)
          cell <- row.lift(cell.index.get)
        } yield cell
        if (actualData.contains(cell.value)) Nil
        else List((actualData, cell.value))
      }
    }
    differencesInNonOverwrittenData shouldBe empty
    ()
  }
  runTests(maxRowsInMemory = None)
  runTests(maxRowsInMemory = None, maxByteArraySize = Some(100000000))
  runTests(maxRowsInMemory = Some(20))
  runTests(maxRowsInMemory = Some(1))
  runTests(maxRowsInMemory = Some(1), maxByteArraySize = Some(100000000))
}

class IntegrationSuiteV1 extends IntegrationSuite("com.crealytics.spark.excel")
class IntegrationSuiteV2 extends IntegrationSuite("excel")
