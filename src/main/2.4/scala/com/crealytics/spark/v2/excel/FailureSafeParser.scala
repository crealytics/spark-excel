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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.catalyst.util._

class FailureSafeParser[IN](
  rawParser: IN => Iterable[InternalRow],
  mode: ParseMode,
  schema: StructType,
  columnNameOfCorruptRecord: String
) {

  private val corruptFieldIndex =
    if (schema.fieldNames.contains(columnNameOfCorruptRecord)) {
      Some(schema.fieldIndex(columnNameOfCorruptRecord))
    } else None

  private val actualSchema = StructType(schema.filterNot(_.name == columnNameOfCorruptRecord))
  private val resultRow = new GenericInternalRow(schema.length)
  private val nullResult = new GenericInternalRow(schema.length)

  // This function takes 2 parameters: an optional partial result, and the bad record. If the given
  // schema doesn't contain a field for corrupted record, we just return the partial result or a
  // row with all fields null. If the given schema contains a field for corrupted record, we will
  // set the bad record to this field, and set other fields according to the partial result or null.
  private val toResultRow: (Option[InternalRow], () => UTF8String) => InternalRow = {
    if (corruptFieldIndex.isDefined) { (row, badRecord) =>
      {
        var i = 0
        while (i < actualSchema.length) {
          val from = actualSchema(i)
          resultRow(schema.fieldIndex(from.name)) = row.map(_.get(i, from.dataType)).orNull
          i += 1
        }
        resultRow(corruptFieldIndex.get) = badRecord()
        resultRow
      }
    } else { (row, _) => row.getOrElse(nullResult) }
  }

  def parse(input: IN): Iterator[InternalRow] = {
    try { rawParser.apply(input).toIterator.map(row => toResultRow(Some(row), () => null)) }
    catch {
      case e: BadRecordException =>
        mode match {
          case PermissiveMode => Iterator(toResultRow(e.partialResult(), e.record))
          case DropMalformedMode => Iterator.empty
          case FailFastMode =>
            throw new SparkException(
              "Malformed records are detected in record parsing. " +
                s"Parse Mode: ${FailFastMode.name}. To process malformed records as null " +
                "result, try setting the option 'mode' as 'PERMISSIVE'.",
              e
            )
        }
    }
  }
}
