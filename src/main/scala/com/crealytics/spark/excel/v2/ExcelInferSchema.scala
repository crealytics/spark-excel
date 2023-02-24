/*
 * Copyright 2023 Martin Mauch (@nightscape)
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

package com.crealytics.spark.excel.v2

import org.apache.poi.ss.usermodel.Cell
import org.apache.poi.ss.usermodel.CellType
import org.apache.poi.ss.usermodel.DateUtil
import org.apache.spark.sql.catalyst.analysis.TypeCoercion
import org.apache.spark.sql.types._

import scala.util.control.Exception.allCatch

class ExcelInferSchema(val options: ExcelOptions) extends Serializable {

  private val timestampParser = ExcelDateTimeStringUtils.getTimestampFormatter(options)

  /** Similar to the JSON schema inference
    *   1. Infer type of each row 2. Merge row types to find common type 3. Replace any null types with string type
    */
  def infer(tokens: Iterator[Vector[Cell]], header: Vector[String]): StructType = {

    /* Possible StructField for row-number column */
    val rowNumField =
      if (options.columnNameOfRowNumber.isDefined) {
        Vector[StructField](StructField(options.columnNameOfRowNumber.get, IntegerType, false))
      } else Vector.empty

    /* Header without row-number-column */
    val dataHeader = if (options.columnNameOfRowNumber.isDefined) header.tail else header

    /* Normal data fields */
    val dataFields =
      if (options.inferSchema) {
        val startType: Vector[DataType] = Vector.fill[DataType](dataHeader.length)(NullType)
        val rootTypes: Vector[DataType] = tokens.foldLeft(startType)(inferRowType)

        toStructFields(rootTypes, dataHeader)
      } else {
        /* By default fields are assumed to be StringType */
        dataHeader.map(fieldName => StructField(fieldName, StringType, nullable = true))
      }

    StructType(rowNumField ++ dataFields)
  }

  private def toStructFields(fieldTypes: Vector[DataType], header: Vector[String]): Vector[StructField] = {
    header.zip(fieldTypes).map { case (thisHeader, rootType) =>
      val dType = rootType match {
        case _: NullType => StringType
        case other => other
      }
      StructField(thisHeader, dType, nullable = true)
    }
  }

  private def inferRowType(rowSoFar: Vector[DataType], next: Vector[Cell]): Vector[DataType] =
    Range(0, rowSoFar.length)
      .map(i =>
        if (i < next.length) inferField(rowSoFar(i), next(i))
        else compatibleType(rowSoFar(i), NullType).getOrElse(StringType)
      )
      .toVector

  /** Infer type of string field. Given known type Double, and a string "1", there is no point checking if it is an Int,
    * as the final type must be Double or higher.
    */
  private def inferTypeOfStringValue(typeSoFar: DataType, value: String): DataType = {
    if (value == null || value.isEmpty || value == options.nullValue) { typeSoFar }
    else {
      val typeElemInfer = typeSoFar match {
        case NullType => tryParseInteger(value)
        case IntegerType => tryParseInteger(value)
        case LongType => tryParseLong(value)
        case _: DecimalType => tryParseDecimal(value)
        case DoubleType => tryParseDouble(value)
        case TimestampType => tryParseTimestamp(value)
        case BooleanType => tryParseBoolean(value)
        case StringType => StringType
        case other: DataType =>
          throw new UnsupportedOperationException(s"Unexpected data type $other")
      }
      compatibleType(typeSoFar, typeElemInfer).getOrElse(StringType)
    }
  }

  /** Infer type of Double value */
  private def inferTypeOfDoubleValue(typeSoFar: DataType, value: Double): DataType = {
    val typeElemInfer =
      if (value.isValidInt) IntegerType
      else if (value == value.longValue()) LongType
      else DoubleType

    compatibleType(typeSoFar, typeElemInfer).getOrElse(DoubleType)
  }

  /** Infer type of Cell field */
  private def inferField(typeSoFar: DataType, field: Cell): DataType = {
    val typeElemInfer = field.getCellType match {
      case CellType.FORMULA =>
        field.getCachedFormulaResultType match {
          case CellType.STRING => inferTypeOfStringValue(typeSoFar, field.getStringCellValue)
          case CellType.NUMERIC => inferTypeOfDoubleValue(typeSoFar, field.getNumericCellValue)
          case _ => NullType
        }
      case CellType.BLANK | CellType.ERROR | CellType._NONE => NullType
      case CellType.BOOLEAN => BooleanType
      case CellType.NUMERIC =>
        if (DateUtil.isCellDateFormatted(field)) TimestampType
        else inferTypeOfDoubleValue(typeSoFar, field.getNumericCellValue)
      case CellType.STRING => inferTypeOfStringValue(typeSoFar, field.getStringCellValue)
    }

    compatibleType(typeSoFar, typeElemInfer).getOrElse(StringType)
  }

  /* Special handling the default locale for backward compatibility */
  private val decimalParser = (s: String) => new java.math.BigDecimal(s)

  private def isInfOrNan(field: String): Boolean = {
    field == options.nanValue || field == options.negativeInf || field == options.positiveInf
  }

  private def tryParseInteger(field: String): DataType =
    if ((allCatch opt field.toInt).isDefined) { IntegerType }
    else { tryParseLong(field) }

  private def tryParseLong(field: String): DataType =
    if ((allCatch opt field.toLong).isDefined) { LongType }
    else { tryParseDecimal(field) }

  private def tryParseDecimal(field: String): DataType = {
    val decimalTry = allCatch opt {
      /* The conversion can fail when the `field` is not a form of number */
      val bigDecimal = decimalParser(field)

      /* Because many other formats do not support decimal, it reduces the cases for decimals by disallowing values
       * having scale (eg. `1.1`).
       */
      if (bigDecimal.scale <= 0) {

        /* `DecimalType` conversion can fail when
         *   1. The precision is bigger than 38. 2. scale is bigger than precision.
         */
        DecimalType(bigDecimal.precision, bigDecimal.scale)
      } else { tryParseDouble(field) }
    }
    decimalTry.getOrElse(tryParseDouble(field))
  }

  private def tryParseDouble(field: String): DataType =
    if ((allCatch opt field.toDouble).isDefined || isInfOrNan(field)) { DoubleType }
    else { tryParseTimestamp(field) }

  /* This case infers a custom `dataFormat` is set */
  private def tryParseTimestamp(field: String): DataType =
    if ((allCatch opt timestampParser.parse(field)).isDefined) { TimestampType }
    else { tryParseBoolean(field) }

  private def tryParseBoolean(field: String): DataType =
    if ((allCatch opt field.toBoolean).isDefined) { BooleanType }
    else { StringType }

  /** Returns the common data type given two input data types so that the return type is compatible with both input data
    * types.
    */
  private def compatibleType(t1: DataType, t2: DataType): Option[DataType] = {
    TypeCoercion.findTightestCommonType(t1, t2).orElse(findCompatibleTypeForExcel(t1, t2))
  }

  /** The following pattern matching represents additional type promotion rules that are Excel specific.
    */
  private val findCompatibleTypeForExcel: (DataType, DataType) => Option[DataType] = {
    case (StringType, _) => Some(StringType)
    case (_, StringType) => Some(StringType)

    /* Double support larger range than fixed decimal, DecimalType.Maximum should be enough in most case, also have
     * better precision.
     */
    case (DoubleType, _: DecimalType) | (_: DecimalType, DoubleType) => Some(DoubleType)

    case (t1: DecimalType, t2: DecimalType) =>
      val scale = math.max(t1.scale, t2.scale)
      val range = math.max(t1.precision - t1.scale, t2.precision - t2.scale)
      if (range + scale > 38) {
        /* DecimalType can't support precision > 38 */
        Some(DoubleType)
      } else { Some(DecimalType(range + scale, scale)) }
    case _ => None
  }
}

object ExcelInferSchema {
  def apply(options: ExcelOptions): ExcelInferSchema = new ExcelInferSchema(options)
}
