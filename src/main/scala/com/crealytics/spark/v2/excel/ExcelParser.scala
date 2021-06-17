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

import org.apache.poi.ss.usermodel.Cell
import org.apache.poi.ss.usermodel.CellType
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.OrderedFilters
import org.apache.spark.sql.catalyst.expressions.ExprUtils
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.LegacyDateFormats.FAST_DATE_FORMAT
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.util.control.NonFatal

/** Constructs a parser for a given schema that translates Excel data to an
  * [[InternalRow]].
  *
  * @param dataSchema The Excel data schema that is specified by the user, or
  *        inferred from underlying data files.
  * @param requiredSchema The schema of the data that should be output for each
  *        row. This should be a subset of the columns in dataSchema.
  * @param options Configuration options for a Excel parser.
  * @param filters The pushdown filters that should be applied to converted
  *        values.
  */
class ExcelParser(dataSchema: StructType, requiredSchema: StructType, val options: ExcelOptions, filters: Seq[Filter])
    extends Logging {
  require(
    requiredSchema.toSet.subsetOf(dataSchema.toSet),
    s"requiredSchema (${requiredSchema.catalogString}) should be the subset of " +
      s"dataSchema (${dataSchema.catalogString})."
  )

  def this(dataSchema: StructType, requiredSchema: StructType, options: ExcelOptions) = {
    this(dataSchema, requiredSchema, options, Seq.empty)
  }
  def this(schema: StructType, options: ExcelOptions) =
    this(schema, schema, options)

  /** A `ValueConverter` is responsible for converting the given value to a
    * desired type.
    */
  private type ValueConverter = Cell => Any

  /** When column pruning is enabled, the parser only parses the required
    * columns based on their positions in the data schema.
    *
    * TODO: make sure that,
    *   parsedSchema == requiredSchema == actualReadDataSchema (dataSchema)
    */
  private val parsedSchema = requiredSchema

  /* Pre-allocated Some to avoid the overhead of building Some per each-row.*/
  private val requiredRow = Some(new GenericInternalRow(requiredSchema.length))

  /** Pre-allocated empty sequence returned when the parsed row cannot pass
    * filters. We preallocate it avoid unnecessary allocations.
    */
  private val noRows = None

  private lazy val timestampFormatter = TimestampFormatter(
    options.timestampFormat,
    options.zoneId,
    options.locale,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = true
  )
  private lazy val dateFormatter =
    DateFormatter(options.dateFormat, options.zoneId, options.locale, legacyFormat = FAST_DATE_FORMAT, isParsing = true)

  /* Excel record is flat, it can use same filters with CSV*/
  private val pushedFilters = new OrderedFilters(filters, requiredSchema)

  /* Retrieve the raw record string.*/
  private def getCurrentInput: UTF8String =
    UTF8String.fromString("TODO: how to show the corrupted record?")

  /** This parser first picks some tokens from the input tokens, according to
    * the required schema, then parse these tokens and put the values in a row,
    * with the order specified by the required schema.
    *
    * For example, let's say there is Excel data as below:
    *  a,b,c
    *  1,2,A
    *
    * So the Excel data schema is: ["a", "b", "c"]
    * And let's say the required schema is: ["c", "b"]
    *
    * with the input tokens,
    *   input tokens - [1, 2, "A"]
    *
    * Each input token is placed in each output row's position by mapping these.
    * In this case,
    *
    *   output row - ["A", 2]
    */
  private val valueConverters: Array[ValueConverter] = {
    requiredSchema
      .map(f => makeConverter(f.name, f.dataType, f.nullable))
      .toArray
  }

  private val decimalParser = ExprUtils.getDecimalParser(options.locale)

  /** Create a converter which converts the Cell value to a value according to a
    * desired type. Currently, we do not support complex types (`ArrayType`,
    * `MapType`, `StructType`).
    *
    * For other nullable types, returns null if it is null or equals to the
    * value specified in `nullValue` option.
    */
  private def makeConverter(name: String, dataType: DataType, nullable: Boolean): ValueConverter =
    dataType match {
      case _: ByteType =>
        (d: Cell) =>
          nullSafeDatum(d, name, nullable, options)(d.getCellType match {
            case CellType.NUMERIC => _.getNumericCellValue.toByte
            case _ => _.getStringCellValue.toByte
          })

      case _: ShortType =>
        (d: Cell) =>
          nullSafeDatum(d, name, nullable, options)(d.getCellType match {
            case CellType.NUMERIC => _.getNumericCellValue.toShort
            case _ => _.getStringCellValue.toShort
          })

      case _: IntegerType =>
        (d: Cell) =>
          nullSafeDatum(d, name, nullable, options)(d.getCellType match {
            case CellType.NUMERIC => _.getNumericCellValue.toInt
            case _ => _.getStringCellValue.toInt
          })

      case _: LongType =>
        (d: Cell) =>
          nullSafeDatum(d, name, nullable, options)(d.getCellType match {
            case CellType.NUMERIC => _.getNumericCellValue.toLong
            case _ => _.getStringCellValue.toLong
          })

      case _: FloatType =>
        (d: Cell) =>
          nullSafeDatum(d, name, nullable, options)(d.getCellType match {
            case CellType.NUMERIC => _.getNumericCellValue.toFloat
            case _ =>
              _.getStringCellValue match {
                case options.nanValue => Float.NaN
                case options.negativeInf => Float.NegativeInfinity
                case options.positiveInf => Float.PositiveInfinity
                case datum => datum.toFloat
              }
          })

      case _: DoubleType =>
        (d: Cell) =>
          nullSafeDatum(d, name, nullable, options)(d.getCellType match {
            case CellType.NUMERIC => _.getNumericCellValue.toDouble
            case _ =>
              ExcelHelper.safeCellStringValue(_) match {
                case options.nanValue => Double.NaN
                case options.negativeInf => Double.NegativeInfinity
                case options.positiveInf => Double.PositiveInfinity
                case s => s.toDouble
              }
          })

      case _: BooleanType =>
        (d: Cell) =>
          nullSafeDatum(d, name, nullable, options)(d.getCellType match {
            case CellType.BOOLEAN => _.getBooleanCellValue
            case _ => _.getStringCellValue.toBoolean
          })

      case dt: DecimalType =>
        (d: Cell) =>
          nullSafeDatum(d, name, nullable, options) { datum =>
            Decimal(decimalParser(datum.getStringCellValue), dt.precision, dt.scale)
          }

      case _: TimestampType =>
        (d: Cell) =>
          nullSafeDatum(d, name, nullable, options) { datum =>
            try {
              timestampFormatter.parse(datum.getStringCellValue)
            } catch {
              case NonFatal(e) =>
                /** If fails to parse, then tries the way used in 2.0 and 1.x
                  * for backwards compatibility.
                  */
                val str = DateTimeUtils.cleanLegacyTimestampStr(UTF8String.fromString(datum.getStringCellValue))
                DateTimeUtils
                  .stringToTimestamp(str, options.zoneId)
                  .getOrElse(throw e)
            }
          }

      case _: DateType =>
        (d: Cell) =>
          nullSafeDatum(d, name, nullable, options) { datum =>
            try {
              dateFormatter.parse(datum.getStringCellValue)
            } catch {
              case NonFatal(e) =>
                /** If fails to parse, then tries the way used in 2.0 and 1.x
                  * for backwards compatibility.
                  */
                val str = DateTimeUtils.cleanLegacyTimestampStr(UTF8String.fromString(datum.getStringCellValue))
                DateTimeUtils
                  .stringToDate(str, options.zoneId)
                  .getOrElse(throw e)
            }
          }

      case _: StringType =>
        (d: Cell) =>
          nullSafeDatum(d, name, nullable, options) { datum =>
            UTF8String.fromString(ExcelHelper.safeCellStringValue(datum))
          }

      case CalendarIntervalType =>
        (d: Cell) =>
          nullSafeDatum(d, name, nullable, options) { datum =>
            IntervalUtils.safeStringToInterval(UTF8String.fromString(datum.getStringCellValue))
          }

      /** We don't actually hit this exception though, we keep it for
        * understandability
        */
      case _ =>
        throw new RuntimeException(s"Unsupported type: ${dataType.typeName}")
    }

  private def nullSafeDatum(datum: Cell, name: String, nullable: Boolean, options: ExcelOptions)(
    converter: ValueConverter
  ): Any = {
    val ret = datum.getCellType match {
      case CellType.BLANK | CellType.ERROR | CellType._NONE => null
      case CellType.STRING =>
        if (datum.getStringCellValue == options.nullValue) null
        else converter.apply(datum)
      case CellType.FORMULA =>
        if (datum.getCachedFormulaResultType == CellType.ERROR) null
        else converter.apply(datum)
      case _ => converter.apply(datum)
    }

    if (ret == null && !nullable)
      throw new RuntimeException(s"null value found but field $name is not nullable.")

    ret
  }

  /** Parses a single Excel string and turns it into either one resulting row or
    * no row (if the the record is malformed).
    */
  val parse: Vector[Cell] => Option[InternalRow] = {
    /* This is intentionally a val to create a function once and reuse.*/
    if (requiredSchema.isEmpty) {
      /* If partition attributes scanned only, `schema` gets empty. */
      (_: Vector[Cell]) => Some(InternalRow.empty)
    } else {
      /* parse if the requiredSchema is nonEmpty*/
      (input: Vector[Cell]) => convert(input)
    }
  }

  private def convert(tokens: Vector[Cell]): Option[InternalRow] = {
    if (tokens == null) {
      throw BadRecordException(() => getCurrentInput, () => None, new RuntimeException("Malformed Excel record"))
    }

    var badRecordException: Option[Throwable] =
      if (tokens.length != parsedSchema.length) {

        /** If the number of tokens doesn't match the schema, we should treat it
          * as a malformed record. However, we still have chance to parse some
          * of the tokens. It continues to parses the tokens normally and sets
          * null when `ArrayIndexOutOfBoundsException` occurs for missing tokens.
          */
        Some(new RuntimeException("Malformed Excel record"))
      } else None

    /** When the length of the returned tokens is identical to the length of the
      * parsed schema, we just need to:
      *  1. Convert the tokens that correspond to the required schema.
      *  2. Apply the pushdown filters to `requiredRow`.
      */
    var i = 0
    val row = requiredRow.get
    var skipRow = false
    while (i < requiredSchema.length) {
      try {
        if (skipRow) {
          row.setNullAt(i)
        } else {
          row(i) = valueConverters(i).apply(tokens(i))
          if (pushedFilters.skipRow(row, i)) {
            skipRow = true
          }
        }
      } catch {
        case NonFatal(e) =>
          badRecordException = badRecordException.orElse(Some(e))
          row.setNullAt(i)
      }
      i += 1
    }
    if (skipRow) {
      noRows
    } else {
      if (badRecordException.isDefined) {
        throw BadRecordException(() => getCurrentInput, () => requiredRow.headOption, badRecordException.get)
      } else {
        requiredRow
      }
    }
  }
}

object ExcelParser {

  /** Parses an iterator that contains Excel list-of-cells and turns it into an
    * iterator of rows.
    */
  def parseIterator(
    rows: Iterator[Vector[Cell]],
    parser: ExcelParser,
    headerChecker: ExcelHeaderChecker,
    schema: StructType
  ): Iterator[InternalRow] = {

    /* Check the header (and pop one record) */
    if (parser.options.header) {
      headerChecker.checkHeaderColumnNames(rows.next().map(_.getStringCellValue()))
    }

    val safeParser = new FailureSafeParser[Vector[Cell]](
      input => parser.parse(input),
      parser.options.parseMode,
      schema,
      parser.options.columnNameOfCorruptRecord
    )
    rows.flatMap(safeParser.parse)
  }
}
