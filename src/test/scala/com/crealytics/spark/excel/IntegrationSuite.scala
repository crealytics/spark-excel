package com.crealytics.spark.excel

import java.io.File
import java.lang.Math.sqrt
import java.sql.Timestamp

import org.scalacheck.{Arbitrary, Gen, Shrink}
import Arbitrary.{arbBigDecimal => _, arbLong => _, arbString => _, _}
import org.scalacheck.ScalacheckShapeless._
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.prop.PropertyChecks
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.scalacheck.Arbitrary.{arbBigDecimal => _, arbLong => _, arbString => _, _}
import org.scalacheck.ScalacheckShapeless._
import org.scalacheck.{Arbitrary, Gen, Shrink}
import org.scalatest.FunSpec
import org.scalatest.prop.PropertyChecks

import scala.util.Random

object IntegrationSuite {

  case class ExampleData(
    aBoolean: Boolean,
    aByte: Byte,
    aShort: Short,
    anInt: Int,
    aLong: Long,
    aFloat: Float,
    aDouble: Double,
    aBigDecimal: BigDecimal,
    aJavaBigDecimal: java.math.BigDecimal,
    aString: String,
    aTimestamp: java.sql.Timestamp,
    aDate: java.sql.Date
  )

  val exampleDataSchema = ScalaReflection.schemaFor[ExampleData].dataType.asInstanceOf[StructType]

  // inferring the schema will not match the original types exactly
  def inferredDataTypes(schema: StructType): Seq[Function[Seq[Any], DataType]] =
    schema.map(_.dataType).map { dt =>
      val pf: Function[Seq[Any], DataType] = dt match {
        case _: DecimalType => {
          case values: Seq[Any] if values.distinct == Seq("") => StringType
          case _ => DoubleType
        }
        case _: NumericType => {
          case _: Seq[Any] => DoubleType
        }
        case DateType => {
          case _: Seq[Any] => TimestampType
        }
        case t: DataType => {
          case _: Seq[Any] => t
        }
      }
      pf
    }

  def expectedDataTypes(inferred: DataFrame): Seq[DataType] =
    inferredDataTypes(exampleDataSchema)
      .to[List]
      .zip(inferred.schema)
      .map {
        case (f, sf) =>
          val values = inferred.select(sf.name).collect().map(_.get(0))
          f(values)
      }

  implicit val arbitraryDateFourDigits = Arbitrary[java.sql.Date](
    Gen
      .chooseNum[Long](0L, (new java.util.Date).getTime + 1000000)
      .map(new java.sql.Date(_))
  )

  implicit val arbitraryTimestamp = Arbitrary[java.sql.Timestamp](
    Gen
      .chooseNum[Long](0L, (new java.util.Date).getTime + 1000000)
      .map(new java.sql.Timestamp(_))
  )

  implicit val arbitraryBigDecimal =
    Arbitrary[BigDecimal](Gen.chooseNum[Double](-1.0e15, 1.0e15).map(BigDecimal.apply))

  implicit val arbitraryJavaBigDecimal =
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

  val rowGen: Gen[ExampleData] = arbitrary[ExampleData]
  val rowsGen: Gen[List[ExampleData]] = Gen.listOf(rowGen)

}

class IntegrationSuite extends FunSpec with PropertyChecks with DataFrameSuiteBase with Matchers {

  import IntegrationSuite._

  import spark.implicits._

  implicit def shrinkOnlyNumberOfRows[A]: Shrink[List[A]] = Shrink.shrinkContainer[List, A]

  val PackageName = "com.crealytics.spark.excel"
  val sheetName = "test sheet"

  def runTests(maxRowsInMemory: Option[Int]) {

    def writeThenRead(
      df: DataFrame,
      schema: Option[StructType] = Some(exampleDataSchema),
      preHeader: Option[String] = None,
      fileName: Option[String] = None
    ): DataFrame = {
      val theFileName = fileName.getOrElse(File.createTempFile("spark_excel_test_", ".xlsx").getAbsolutePath)

      val writer = df.write
        .format(PackageName)
        .option("sheetName", sheetName)
        .option("useHeader", "true")
        .mode("overwrite")
      val withPreHeader = preHeader.foldLeft(writer) {
        case (wri, pre) =>
          wri.option("preHeader", pre)
      }
      withPreHeader.save(theFileName)

      val reader = spark.read
        .format(PackageName)
        .option("sheetName", sheetName)
        .option("useHeader", "true")
        .option("treatEmptyValuesAsNulls", "true")
        .option("addColorColumns", "false")
      val withPreHeaderSkip = preHeader.foldLeft(reader) {
        case (reader, preHeader) =>
          val skipRows = preHeader.count(_ == '\n') + 1
          reader.option("skipFirstRows", skipRows)
      }
      val withSchema = schema.foldLeft(withPreHeaderSkip.option("inferSchema", schema.isEmpty))(_ schema _)
      val withStreaming = maxRowsInMemory.foldLeft(withSchema) {
        case (reader, maxRowsInMem) => reader.option("maxRowsInMemory", maxRowsInMem)
      }
      withStreaming.load(theFileName)
    }

    def assertEqualAfterInferringTypes(original: DataFrame, inferred: DataFrame) = {
      val originalWithInferredColumnTypes =
        original.schema
          .zip(expectedDataTypes(inferred))
          .foldLeft(original) {
            case (df, (field, dataType)) =>
              df.withColumn(field.name, df(field.name).cast(dataType))
          }
      val expected = spark.createDataFrame(originalWithInferredColumnTypes.rdd, inferred.schema)
      assertDataFrameEquals(expected, inferred)
    }

    describe(s"with maxRowsInMemory = $maxRowsInMemory") {
      it("parses known datatypes correctly") {
        forAll(rowsGen, MinSuccessful(20)) { rows =>
          val expected = spark.createDataset(rows).toDF
          val actual = writeThenRead(expected)
          assertDataFrameApproximateEquals(expected, actual, relTol = 1.0E-6)
        }
      }

      it("handles null values correctly") {
        forAll(rowsGen, MinSuccessful(20)) { rows =>
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

          assertDataFrameApproximateEquals(expectedWithEmptyStr, writeThenRead(expectedWithNull), relTol = 1.0E-6)
        }
      }

      it("infers schema correctly") {
        forAll(rowsGen, MinSuccessful(20)) { rows =>
          val df = spark.createDataset(rows).toDF
          val inferred = writeThenRead(df, schema = None)

          if (df.count() == 0) {
            // Without actual data, we assume everything is a StringType
            assert(inferred.schema.fields.forall(_.dataType == StringType))
          } else {
            val actualDataTypes = inferred.schema.fields.map(_.dataType).to[List]
            assert(actualDataTypes, expectedDataTypes(inferred))
          }
        }
      }

      it("returns all data rows when inferring schema") {
        forAll(rowsGen.filter(!_.isEmpty), MinSuccessful(20)) { rows =>
          val original = spark.createDataset(rows).toDF
          val inferred = writeThenRead(original, schema = None)
          assertEqualAfterInferringTypes(original, inferred)
        }
      }

      implicit val shrinkNoString: Shrink[String] = Shrink(_ => Stream.empty)
      val preHeaderGen: Gen[String] = for {
        words <- Gen.containerOf[Vector, String](arbitrary[String].filterNot(_.isEmpty))
        newLines <- Gen.containerOf[Vector, String](Gen.oneOf("\n", "\r\n"))
        tabs <- Gen.containerOf[Vector, String](Gen.const("\t"))
      } yield Random.shuffle(newLines ++ tabs ++ words).mkString

      it("correctly writes and skips the preHeader") {
        forAll(rowsGen.filter(!_.isEmpty), preHeaderGen, MinSuccessful(10)) {
          case (rows, preHeader) =>
            val fileName = File.createTempFile("spark_excel_test_", ".xlsx").getAbsolutePath
            val original = spark.createDataset(rows).toDF
            val inferred =
              writeThenRead(original, schema = None, preHeader = Some(preHeader), fileName = Some(fileName))

            val preHeaderLines = preHeader.split("\\R", -1).map(_.split("\t"))
            val firstRowsDf = spark.read
              .format(PackageName)
              .option("sheetName", sheetName)
              .option("useHeader", "false")
              .option("excerptSize", preHeaderLines.size)
              .load(fileName)
            val actualHeaders = firstRowsDf
              .limit(preHeaderLines.length)
              .collect()
              .map(_.toSeq.filter(_ != null))

            actualHeaders should contain theSameElementsInOrderAs preHeaderLines

            assertEqualAfterInferringTypes(original, inferred)
        }

      }

      it("handles multi-line column headers correctly") {
        forAll(rowsGen.filter(!_.isEmpty), MinSuccessful(20)) { rows =>
          val original = spark.createDataset(rows).toDF
          val multiLineHeaders = original.withColumnRenamed("aString", "a\nString")
          val inferred = writeThenRead(multiLineHeaders, schema = None)
          assertEqualAfterInferringTypes(multiLineHeaders, inferred)
        }
      }
    }
  }

  runTests(maxRowsInMemory = None)
  runTests(maxRowsInMemory = Some(20))
}
