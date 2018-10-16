package com.crealytics.spark.excel
import java.sql.{Date, Timestamp}
import java.time.{Instant, ZoneId, ZoneOffset}
import java.time.temporal.ChronoUnit

import org.scalacheck.Arbitrary.{arbBigDecimal => _, arbLong => _, arbString => _, _}
import org.scalacheck.ScalacheckShapeless._
import com.norbitltd.spoiwo.model.{Cell, CellRange, Sheet, Row => SRow, Table => STable}
import org.apache.poi.ss.util.CellReference
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.scalacheck.{Arbitrary, Gen}

import scala.collection.JavaConverters._
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

trait Generators {

  val exampleDataSchema = ScalaReflection.schemaFor[ExampleData].dataType.asInstanceOf[StructType]

  private val dstTransitionDays =
    ZoneId.systemDefault().getRules.getTransitions.asScala.map(_.getInstant.truncatedTo(ChronoUnit.DAYS))
  def isDstTransitionDay(instant: Instant): Boolean = dstTransitionDays.contains(instant.truncatedTo(ChronoUnit.DAYS))
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
  val cellAddressGen = for {
    row <- Gen.choose(0, 100)
    col <- Gen.choose(0, 100)
  } yield new CellReference(row, col)

  val sheetName = "test sheet"

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

  val tableName = "TestTable"

  val sheetWithTableGen = for {
    sheet <- sheetGen
    startCellAddress <- cellAddressGen
    width <- Gen.choose(0, 50)
    height <- Gen.choose(0, 200)
  } yield
    sheet.withTables(
      STable(
        cellRange = CellRange(
          rowRange = (startCellAddress.getRow, startCellAddress.getRow + height),
          columnRange = (startCellAddress.getCol, startCellAddress.getCol + width)
        ),
        name = tableName,
        displayName = tableName
      )
    )

  val dataAndLocationGen = for {
    rows <- rowsGen
    startAddress <- cellAddressGen
  } yield
    (
      rows,
      startAddress,
      new CellReference(startAddress.getRow + rows.size, startAddress.getCol + exampleDataSchema.size - 1)
    )
}
