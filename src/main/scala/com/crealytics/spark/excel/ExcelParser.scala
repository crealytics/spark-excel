/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.crealytics.spark.excel

import java.math.BigDecimal
import java.sql.Timestamp

import org.apache.poi.ss.usermodel.{Cell, CellType, DataFormatter, DateUtil}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{BadRecordException, DateTimeUtils}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

/**
  * Constructs a parser for a given schema that translates Excel data to an [[InternalRow]].
  *
  * @param dataSchema The Excel data schema that is specified by the user, or inferred from underlying
  *                   data files.
  * @param requiredSchema The schema of the data that should be output for each row. This should be a
  *                       subset of the columns in dataSchema.
  * @param options Configuration options for a Excel parser.
  */
class ExcelParser(dataSchema: StructType, requiredSchema: StructType, val options: ExcelOptions) extends Logging {
  require(
    requiredSchema.toSet.subsetOf(dataSchema.toSet),
    s"requiredSchema (${requiredSchema.catalogString}) should be the subset of " +
      s"dataSchema (${dataSchema.catalogString})."
  )

  def this(schema: StructType, options: ExcelOptions) = this(schema, schema, options)

  // A `ValueConverter` is responsible for converting the given value to a desired type.
  private type ValueConverter = Cell => Option[Any]

  // This index is used to reorder parsed cells
  private val cellIndexArr =
    requiredSchema.map(f => java.lang.Integer.valueOf(dataSchema.indexOf(f))).toArray

  // When column pruning is enabled, the parser only parses the required columns based on
  // their positions in the data schema.
  private val parsedSchema = if (options.columnPruning) requiredSchema else dataSchema

  private val row = new GenericInternalRow(requiredSchema.length)

  // This parser first picks some cells from the input cells, according to the required schema,
  // then parse these cells and put the values in a row, with the order specified by the required
  // schema.
  //
  // For example, let's say there is Excel data as below:
  //
  //   a,b,c
  //   1,2,A
  //
  // So the Excel data schema is: ["a", "b", "c"]
  // And let's say the required schema is: ["c", "b"]
  //
  // with the input cells,
  //
  //   input cells - [1, 2, "A"]
  //
  // Each input cell is placed in each output row's position by mapping these. In this case,
  //
  //   output row - ["A", 2]
  private val valueConverters: Array[Cell => Any] = {
    requiredSchema.map(f => makeConverter(f.name, f.dataType, f.nullable, options)).toArray
  }

  /**
    * Create a converter which converts the string value to a value according to a desired type.
    * Currently, we do not support complex types (`ArrayType`, `MapType`, `StructType`).
    *
    * For other nullable types, returns null if it is null or equals to the value specified
    * in `nullValue` option.
    */
  def makeConverter(name: String, dataType: DataType, nullable: Boolean = true, options: ExcelOptions): Cell => Any =
    dataType match {

      case _: ByteType => (d: Cell) => nullSafeCell(d, name, nullable, options)(_.numericValue.map(_.toByte))

      case _: ShortType => (d: Cell) => nullSafeCell(d, name, nullable, options)(_.numericValue.map(_.toShort))

      case _: IntegerType => (d: Cell) => nullSafeCell(d, name, nullable, options)(_.numericValue.map(_.toInt))

      case _: LongType => (d: Cell) => nullSafeCell(d, name, nullable, options)(_.numericValue.map(_.toLong))

      case _: FloatType => (d: Cell) => nullSafeCell(d, name, nullable, options)(_.numericValue.map(_.toFloat))

      case _: DoubleType => (d: Cell) => nullSafeCell(d, name, nullable, options)(_.numericValue)

      case _: BooleanType => (d: Cell) => nullSafeCell(d, name, nullable, options)(_.booleanValue)

      case dt: DecimalType =>
        (d: Cell) =>
          nullSafeCell(d, name, nullable, options)(_.bigDecimalValue.map { value =>
            Decimal(value, dt.precision, dt.scale)
          })

      case dt: DecimalType =>
        (d: Cell) =>
          if (d.getCellType == CellType.STRING && d.getStringCellValue == "") None
          else d.bigDecimalValue.map(Decimal(_, dt.precision, dt.scale))

      case _: TimestampType =>
        (d: Cell) =>
          nullSafeCell(d, name, nullable, options) { cell =>
            cell.getCellType match {
              case CellType.BLANK => None
              case CellType.NUMERIC => cell.numericValue.map(n => DateUtil.getJavaDate(n).getTime * 1000L)
              case CellType.STRING =>
                cell.stringValue
                  .filter(_.trim.nonEmpty)
                  .map(
                    datum =>
                      // This one will lose microseconds parts.
                      // See https://issues.apache.org/jira/browse/SPARK-10681.
                      Try(options.timestampParser(datum).getTime * 1000L)
                        .getOrElse {
                          // If it fails to parse, then tries the way used in 2.0 and 1.x for backwards
                          // compatibility.
                          DateTimeUtils.stringToTime(datum).getTime * 1000L
                        }
                  )

            }
          }

      case _: DateType =>
        (d: Cell) =>
          nullSafeCell(d, name, nullable, options)(
            _.numericValue.map(n => DateTimeUtils.millisToDays(DateUtil.getJavaDate(n).getTime))
          )

      case _: StringType =>
        (d: Cell) =>
          nullSafeCell(d, name, nullable, options)(
            _.stringValue.filterNot(_.isEmpty && options.treatEmptyValuesAsNulls).map(UTF8String.fromString)
          )

      // We don't actually hit this exception though, we keep it for understandability
      case _ => throw new RuntimeException(s"Unsupported type: ${dataType.typeName}")
    }

  private def nullSafeCell(datum: Cell, name: String, nullable: Boolean, options: ExcelOptions)(
    converter: ValueConverter
  ): Any = {
    val valueOption = Option(datum).flatMap(converter).filterNot(_ == options.nullValue)
    if (!nullable && valueOption.isEmpty) {
      throw new RuntimeException(s"null value found but field $name is not nullable.")
    }
    valueOption.orNull
  }

  private val doParse = if (requiredSchema.nonEmpty) { (input: Array[Cell]) => convert(input) }
  else {
    // If `columnPruning` enabled and partition attributes scanned only,
    // `schema` gets empty.
    (_: Array[Cell]) => InternalRow.empty
  }

  /**
    * Parses a single Excel row and turns it into either one resulting row or no row (if the
    * the record is malformed).
    */
  def parse(input: Array[Cell]): InternalRow = doParse(input)

  private val getCell = if (options.columnPruning) { (cells: Array[Cell], index: Int) => cells(index) }
  else { (cells: Array[Cell], index: Int) => cells(cellIndexArr(index)) }

  private def convert(cells: Array[Cell]): InternalRow = {
    if (cells == null) {
      throw BadRecordException(
        () => UTF8String.fromString("TODO"),
        () => None,
        new RuntimeException("Malformed Excel record")
      )
    } else if (cells.length != parsedSchema.length) {
      // If the number of cells doesn't match the schema, we should treat it as a malformed record.
      // However, we still have chance to parse some of the cells, by adding extra null cells in
      // the tail if the number is smaller, or by dropping extra cells if the number is larger.
      val checkedCells = if (parsedSchema.length > cells.length) {
        cells ++ new Array[Cell](parsedSchema.length - cells.length)
      } else {
        cells.take(parsedSchema.length)
      }
      def getPartialResult(): Option[InternalRow] = {
        try {
          Some(convert(checkedCells))
        } catch {
          case _: BadRecordException => None
        }
      }
      // For records with less or more cells than the schema, tries to return partial results
      // if possible.
      throw BadRecordException(
        () => UTF8String.fromString("TODO"),
        () => getPartialResult(),
        new RuntimeException(s"Malformed Excel record: ${getPartialResult()}")
      )
    } else {
      try {
        // When the length of the returned cells is identical to the length of the parsed schema,
        // we just need to convert the cells that correspond to the required columns.
        var i = 0
        while (i < requiredSchema.length) {
          row(i) = valueConverters(i).apply(getCell(cells, i))
          i += 1
        }
        row
      } catch {
        case NonFatal(e) =>
          // For corrupted records with the number of cells same as the schema,
          // Excel reader doesn't support partial results. All fields other than the field
          // configured by `columnNameOfCorruptRecord` are set to `null`.
          throw BadRecordException(() => UTF8String.fromString("TODO"), () => None, e)
      }
    }
  }
}
