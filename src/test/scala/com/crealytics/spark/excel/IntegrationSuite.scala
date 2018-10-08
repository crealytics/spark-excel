package com.crealytics.spark.excel

import java.io.{File, FileOutputStream}
import java.sql.{Date, Timestamp}
import java.time.temporal.ChronoUnit
import java.time.{Instant, ZoneId, ZoneOffset}

import cats.Monoid
import cats.instances.all._
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.norbitltd.spoiwo.model.{Cell, CellRange, Sheet, Row => SRow, Table => STable}
import com.norbitltd.spoiwo.natures.xlsx.Model2XlsxConversions._
import org.apache.poi.ss.util.CellReference
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.scalacheck.Arbitrary.{arbBigDecimal => _, arbLong => _, arbString => _, _}
import org.scalacheck.ScalacheckShapeless._
import org.scalacheck.{Arbitrary, Gen, Shrink}
import org.scalactic.anyvals.PosInt
import org.scalatest.prop.PropertyChecks
import org.scalatest.{FunSpec, Matchers}

import scala.collection.JavaConverters._
import scala.util.Random

object IntegrationSuite {

  case class ExampleData(
    aBoolean: Boolean,
    aBooleanOption: Option[Boolean],
    aByte: Byte,
    aByteOption: Option[Byte],
    aShort: Short,
    aShortOption: Option[Short],
    anInt: Int,
    anIntOption: Option[Int],
    aLong: Long,
    aLongOption: Option[Long],
    aFloat: Float,
    aFloatOption: Option[Float],
    aDouble: Double,
    aDoubleOption: Option[Double],
    aBigDecimal: BigDecimal,
    aBigDecimalOption: Option[BigDecimal],
    aJavaBigDecimal: java.math.BigDecimal,
    aJavaBigDecimalOption: Option[java.math.BigDecimal],
    aString: String,
    aStringOption: Option[String],
    aTimestamp: java.sql.Timestamp,
    aTimestampOption: Option[java.sql.Timestamp],
    aDate: java.sql.Date,
    aDateOption: Option[java.sql.Date]
  )

  private val exampleDataSchema = ScalaReflection.schemaFor[ExampleData].dataType.asInstanceOf[StructType]

  // inferring the schema will not match the original types exactly
  def inferredDataTypes(schema: StructType): Seq[Function[Seq[Any], DataType]] =
    schema.map(_.dataType).map { dt =>
      val pf: Function[Seq[Any], DataType] = dt match {
        case _: DecimalType => {
          case values: Seq[Any] if values.distinct == Seq("") => StringType
          case _ => DoubleType
        }
        case _: NumericType => _: Seq[Any] => DoubleType
        case DateType => _: Seq[Any] => TimestampType
        case t: DataType => _: Seq[Any] => t
      }
      pf
    }

  def expectedDataTypes(inferred: DataFrame): Seq[(String, DataType)] = {
    val data = inferred.collect()
    inferredDataTypes(exampleDataSchema)
      .to[List]
      .zip(inferred.schema)
      .zipWithIndex
      .map { case ((f, sf), idx) => sf.name -> f(data.map(_.get(idx))) }
  }

  val dstTransitionDays =
    ZoneId.systemDefault().getRules.getTransitions.asScala.map(_.getInstant.truncatedTo(ChronoUnit.DAYS))
  def isDstTransitionDay(instant: Instant): Boolean = {
    dstTransitionDays.exists(_ == instant.truncatedTo(ChronoUnit.DAYS))
  }
  implicit val arbitraryDateFourDigits: Arbitrary[Date] = Arbitrary[java.sql.Date](
    Gen
      .chooseNum[Long](0L, (new java.util.Date).getTime + 1000000)
      .map(new java.sql.Date(_))
      // We get some weird DST problems when the chosen date is a DST transition
      .filterNot(d => isDstTransitionDay(d.toLocalDate.atStartOfDay(ZoneOffset.UTC).toInstant))
  )

  implicit val arbitraryTimestamp: Arbitrary[Timestamp] = Arbitrary[java.sql.Timestamp](
    Gen
      .chooseNum[Long](0L, (new java.util.Date).getTime + 1000000)
      .map(new java.sql.Timestamp(_))
      // We get some weird DST problems when the chosen date is a DST transition
      .filterNot(d => isDstTransitionDay(d.toInstant))
  )

  implicit val arbitraryBigDecimal: Arbitrary[BigDecimal] =
    Arbitrary[BigDecimal](Gen.chooseNum[Double](-1.0e15, 1.0e15).map(BigDecimal.apply))

  implicit val arbitraryJavaBigDecimal: Arbitrary[java.math.BigDecimal] =
    Arbitrary[java.math.BigDecimal](arbitraryBigDecimal.arbitrary.map(_.bigDecimal))

  // Unfortunately we're losing some precision when parsing Longs
  // due to the fact that we have to read them as Doubles and then cast.
  // We're restricting our tests to Int-sized Longs in order not to fail
  // because of this issue.
  implicit val arbitraryLongWithLosslessDoubleConvertability: Arbitrary[Long] =
    Arbitrary[Long] {
      arbitrary[Int].map(_.toLong)
    }

  implicit val arbitraryStringWithoutUnicodeCharacters: Arbitrary[String] =
    Arbitrary[String](Gen.alphaNumStr)

  val rowGen: Gen[ExampleData] = arbitrary[ExampleData].map(d => if (d.aString.isEmpty) d.copy(aString = null) else d)
  val rowsGen: Gen[List[ExampleData]] = Gen.listOf(rowGen)

}

class IntegrationSuite extends FunSpec with PropertyChecks with DataFrameSuiteBase with Matchers {

  import IntegrationSuite._
  import spark.implicits._

  implicit def shrinkOnlyNumberOfRows[A]: Shrink[List[A]] = Shrink.shrinkContainer[List, A]
  implicit override val generatorDrivenConfig: PropertyCheckConfiguration = PropertyCheckConfiguration(
    minSuccessful = PosInt.from(sys.env.getOrElse("EXAMPLES_PER_PROPERTY", "6").toInt).get
  )
  val sheetName = "test sheet"

  def runTests(maxRowsInMemory: Option[Int]) {

    def writeThenRead(
      df: DataFrame,
      schema: Option[StructType] = Some(exampleDataSchema),
      fileName: Option[String] = None,
      saveMode: SaveMode = SaveMode.Overwrite,
      tableName: Option[String] = None,
      dataAddress: Option[String] = None
    ): DataFrame = {
      val theFileName = fileName.getOrElse(File.createTempFile("spark_excel_test_", ".xlsx").getAbsolutePath)

      val writer = df.write
        .excel(sheetName = sheetName, useHeader = true)
        .mode(saveMode)
      val configuredWriter =
        Map("dataAddress" -> dataAddress, "tableName" -> tableName).foldLeft(writer) {
          case (wri, (key, Some(value))) => wri.option(key, value)
          case (wri, _) => wri
        }
      configuredWriter.save(theFileName)

      val reader = spark.read
        .excel(sheetName = sheetName, useHeader = true, treatEmptyValuesAsNulls = false, addColorColumns = false)
      val skipRows = dataAddress
        .map(a => AddressContainer(a).startRow)
        .getOrElse(0)
      val configuredReader = Map(
        "maxRowsInMemory" -> maxRowsInMemory,
        "inferSchema" -> Some(schema.isEmpty),
        "excerptSize" -> Some(skipRows + 10),
        "tableName" -> tableName,
        "dataAddress" -> dataAddress.orElse(Some("A" + (skipRows + 1)))
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
          .foldLeft(original) {
            case (df, (field, dataType)) =>
              df.withColumn(field.name, df(field.name).cast(dataType))
          }
      val expected = spark.createDataFrame(originalWithInferredColumnTypes.rdd, inferred.schema)
      assertDataFrameEquals(expected, inferred)
    }

    describe(s"with maxRowsInMemory = $maxRowsInMemory") {
      it("parses known datatypes correctly") {
        forAll(rowsGen) { rows =>
          val expected = spark.createDataset(rows).toDF
          val actual = writeThenRead(expected)
          assertDataFrameApproximateEquals(expected, actual, relTol = 1.0E-6)
        }
      }

      it("reads blank cells as null and empty string cells as \"\"") {
        forAll(rowsGen) { rows =>
          val expected = spark.createDataset(rows).toDF

          // We need two dataframes, one with null values, one with empty strings.
          // This is because we want ExcelFileSaver to write an empty string
          // if there's a null in that column.
          // expectedWithEmptyStr is what the dataframe should look
          // like when the Excel spreadsheet is saved.
          val expectedWithNull = expected.withColumn("aString", lit(null: String))
          // Generate the same DataFrame but with empty strings
          val expectedWithEmptyStr = expected.withColumn("aString", lit("": String))
          // Set the schema so that aString is nullable
          val fields = expectedWithEmptyStr.schema.fields
          fields.update(fields.indexWhere(_.name == "aString"), StructField("aString", DataTypes.StringType, true))

          assertDataFrameApproximateEquals(expectedWithEmptyStr, writeThenRead(expectedWithEmptyStr), relTol = 1.0E-6)
        }
      }

      it("infers schema correctly") {
        forAll(rowsGen) { rows =>
          val df = spark.createDataset(rows).toDF
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
          val original = spark.createDataset(rows).toDF
          val inferred = writeThenRead(original, schema = None)
          assertEqualAfterInferringTypes(original, inferred)
        }
      }

      it("handles multi-line column headers correctly") {
        forAll(rowsGen.filter(_.nonEmpty)) { rows =>
          val original = spark.createDataset(rows).toDF
          val multiLineHeaders = original.withColumnRenamed("aString", "a\nString")
          val inferred = writeThenRead(multiLineHeaders, schema = None)
          assertEqualAfterInferringTypes(multiLineHeaders, inferred)
        }
      }

      it("reads files with missing cells correctly") {
        forAll(rowsGen.filter(_.nonEmpty)) { rows =>
          val fileName = File.createTempFile("spark_excel_test_", ".xlsx").getAbsolutePath
          val numCols = 50
          val headerNames = (0 until numCols).map(c => s"header_$c")
          val existingData = Sheet(
            name = sheetName,
            rows =
              SRow(headerNames.zipWithIndex.map { case (header, c) => Cell(header, index = c) }, index = 0) ::
                (0 until 100)
                .map(r => SRow((0 until numCols).filter(_ % 2 == 0).map(c => Cell(s"$r,$c", index = c)), index = r + 1))
                .to[List]
          )
          existingData.convertAsXlsx.write(new FileOutputStream(new File(fileName)))
          val allData = spark.read
            .excel(sheetName = sheetName, useHeader = true, inferSchema = true)
            .load(fileName)
          allData.schema.fieldNames should equal(headerNames)
          val (headersWithData, headersWithoutData) = headerNames.zipWithIndex.partition(_._2 % 2 == 0)
          val expectedContents = existingData.rows.drop(1).map(_.cells.map(_.value))
          val actualContents = allData.select(headersWithData.map(c => col(c._1)): _*).collect().map(_.toSeq)
          actualContents should contain theSameElementsInOrderAs expectedContents
          val emptyContents = allData.select(headersWithoutData.map(c => col(c._1)): _*).collect().map(_.toSeq)
          emptyContents should contain theSameElementsInOrderAs (0 until existingData.rows.size - 1)
            .map(_ => headersWithoutData.map(_ => null))
        }
      }

      val cellAddressGen = for {
        row <- Gen.choose(0, 100)
        col <- Gen.choose(0, 100)
      } yield new CellReference(row, col)

      val sheetGen = for {
        numRows <- Gen.choose(0, 200)
        numCols <- Gen.choose(0, 200)
      } yield {
        Sheet(
          name = sheetName,
          rows = (0 until numRows)
            .map(r => SRow((0 until numCols).map(c => Cell(s"$r,$c", index = c)), index = r))
            .to[List]
        )
      }

      val dataAndLocationGen = for {
        rows <- rowsGen
        startAddress <- cellAddressGen
      } yield
        (
          rows,
          startAddress,
          new CellReference(startAddress.getRow + rows.size, startAddress.getCol + exampleDataSchema.size - 1)
        )

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
            withFileOutputStream(fileName)(existingData.convertAsXlsx.write)
            val original = spark.createDataset(rows).toDF
            val inferred =
              writeThenRead(
                original,
                schema = None,
                fileName = Some(fileName),
                saveMode = SaveMode.Append,
                dataAddress = Some(s"${startCellAddress.formatAsString()}:${endCellAddress.formatAsString()}")
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
              val original = spark.createDataset(rows).toDF

              val existingData = sheet.withTables(
                STable(
                  cellRange = CellRange(
                    rowRange = (startCellAddress.getRow, endCellAddress.getRow),
                    columnRange = (startCellAddress.getCol, endCellAddress.getCol)
                  ),
                  name = tableName,
                  displayName = tableName
                )
              )
              withFileOutputStream(fileName)(existingData.convertAsXlsx.write)
              val inferred =
                writeThenRead(
                  original,
                  schema = None,
                  fileName = Some(fileName),
                  saveMode = SaveMode.Append,
                  tableName = Some(tableName)
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
        row.cells.filterNot(
          c =>
            c.index.get >= startCellAddress.getCol &&
              c.index.get <= endCellAddress.getCol &&
              row.index.get >= startCellAddress.getRow &&
              row.index.get <= endCellAddress.getRow
        )
      )
    })
    val allData = spark.read
      .excel(sheetName = sheetName, useHeader = false, inferSchema = false)
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
  }
  runTests(maxRowsInMemory = None)
  runTests(maxRowsInMemory = Some(20))
}
