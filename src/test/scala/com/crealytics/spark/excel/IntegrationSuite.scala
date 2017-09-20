package com.crealytics.spark.excel

import java.io.File
import java.sql.Timestamp

import org.scalacheck.{Arbitrary, Gen, Shrink}
import Arbitrary.{arbLong => _, arbString => _, _}
import org.scalacheck.ScalacheckShapeless._
import org.scalatest.FunSuite
import org.scalatest.prop.PropertyChecks
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.lit

object IntegrationSuite {

  case class ExampleData(
                          aBoolean: Boolean,
                          aByte: Byte,
                          aShort: Short,
                          anInt: Int,
                          aLong: Long,
                          aDouble: Double,
                          aString: String,
                          aTimestamp: java.sql.Timestamp,
                          aDate: java.sql.Date
                        )

  val exampleDataSchema = ScalaReflection.schemaFor[ExampleData].dataType.asInstanceOf[StructType]

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

  // Unfortunately we're losing some precision when parsing Longs
  // due to the fact that we have to read them as Doubles and then cast.
  // We're restricting our tests to Int-sized Longs in order not to fail
  // because of this issue.
  implicit val arbitraryLongWithLosslessDoubleConvertability: Arbitrary[Long] =
    Arbitrary[Long] { arbitrary[Int].map(_.toLong) }

  implicit val arbitraryStringWithoutUnicodeCharacters: Arbitrary[String] =
    Arbitrary[String](Gen.alphaNumStr)

  val rowGen: Gen[ExampleData] = arbitrary[ExampleData]
  val rowsGen: Gen[List[ExampleData]] = Gen.listOf(rowGen)
}

class IntegrationSuite extends FunSuite with PropertyChecks with DataFrameSuiteBase {
  import IntegrationSuite._

  import spark.implicits._

  implicit def shrinkOnlyNumberOfRows[A]: Shrink[List[A]] = Shrink.shrinkContainer[List, A]

  val PackageName = "com.crealytics.spark.excel"
  val sheetName = "test sheet"

  def writeThenRead(df: DataFrame, schema: Option[StructType] = Some(exampleDataSchema)): DataFrame = {
    val fileName = File.createTempFile("spark_excel_test_", ".xlsx").getAbsolutePath

    df.write
      .format(PackageName)
      .option("sheetName", sheetName)
      .option("useHeader", "true")
      .mode("overwrite")
      .save(fileName)

    val reader = spark.read.format(PackageName)
      .option("sheetName", sheetName)
      .option("useHeader", "true")
      .option("treatEmptyValuesAsNulls", "true")
      .option("addColorColumns", "false")

    if (schema.isDefined) {
      reader.option("inferSchema", false).schema(schema.get).load(fileName)
    } else {
      reader.option("inferSchema", true).load(fileName)
    }
  }

  test("parses known datatypes correctly") {
    forAll(rowsGen, MinSuccessful(20)) { rows =>
      val expected = spark.createDataset(rows).toDF

      assertDataFrameEquals(expected, writeThenRead(expected))
    }
  }

  test("handles null values correctly") {
    forAll(rowsGen, MinSuccessful(20)) { rows =>
      val expected = spark.createDataset(rows).toDF

      // We need two dataframes, one with null values, one with empty strings, this is because we want ExcelFileSaver to
      // write an empty string, if there's a null in that column. expectedWithEmptyStr is what the dataframe should look
      // like when the Excel spreadsheet is saved
      val expectedWithNull = expected.withColumn("aString", lit(null: String))
      // Generate the same DataFrame but with empty strings
      val expectedWithEmptyStr = expected.withColumn("aString", lit("": String))
      // Set the schema so that aString is nullable
      expectedWithEmptyStr.schema.fields.update(6, StructField("aString", DataTypes.StringType, true))

      assertDataFrameEquals(expectedWithEmptyStr, writeThenRead(expectedWithNull))
    }
  }

  test("infers schema correctly") {
    forAll(rowsGen, MinSuccessful(20)) { rows =>
      val df = spark.createDataset(rows).toDF
      val inferred = writeThenRead(df, schema = None)

      assert(if (df.count() > 0) BooleanType else StringType, inferred.schema.fields(0).dataType)
      assert(if (df.count() > 0) DoubleType else StringType, inferred.schema.fields(1).dataType)
      assert(if (df.count() > 0) DoubleType else StringType, inferred.schema.fields(2).dataType)
      assert(if (df.count() > 0) DoubleType else StringType, inferred.schema.fields(3).dataType)
      assert(if (df.count() > 0) DoubleType else StringType, inferred.schema.fields(4).dataType)
      assert(if (df.count() > 0) DoubleType else StringType, inferred.schema.fields(5).dataType)
      assert(if (df.count() > 0) StringType else StringType, inferred.schema.fields(6).dataType)
      assert(if (df.count() > 0) TimestampType else StringType, inferred.schema.fields(7).dataType)
      assert(if (df.count() > 0) TimestampType else StringType, inferred.schema.fields(8).dataType)
    }
  }
}
