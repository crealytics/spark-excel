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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

/** Checks that column names in a Excel header and field names in the schema are
  * the same by taking into account case sensitivity.
  *
  * @param schema provided (or inferred) schema to which Excel must conform.
  * @param options parsed Excel options.
  * @param source name of Excel source that are currently checked. It is used in
  * error messages.
  */
class ExcelHeaderChecker(
    schema: StructType,
    options: ExcelOptions,
    source: String
) extends Logging {

  /** Indicates if it is set to `false`, comparison of column names and schema
    * field names is not case sensitive.
    */
  private val caseSensitive = SQLConf.get.caseSensitiveAnalysis

  /** Indicates if it is `true`, column names are ignored otherwise the Excel
    * column names are checked for conformance to the schema. In the case if
    * the column name don't conform to the schema, an exception is thrown.
    */
  private val enforceSchema = options.enforceSchema

  /** Checks that column names in a Excel header and field names in the schema
    * are the same by taking into account case sensitivity.
    *
    * @param columnNames names of Excel columns that must be checked against to
    * the schema.
    */
  def checkHeaderColumnNames(columnNames: Vector[String]): Unit = {
    if (columnNames != null) {
      val fieldNames                   = schema.map(_.name).toIndexedSeq
      val (headerLen, schemaSize)      = (columnNames.size, fieldNames.length)
      var errorMessage: Option[String] = None

      if (headerLen == schemaSize) {
        var i = 0
        while (errorMessage.isEmpty && i < headerLen) {
          var (nameInSchema, nameInHeader) = (fieldNames(i), columnNames(i))
          if (!caseSensitive) {
            // scalastyle:off caselocale
            nameInSchema = nameInSchema.toLowerCase
            nameInHeader = nameInHeader.toLowerCase
            // scalastyle:on caselocale
          }
          if (nameInHeader != nameInSchema) {
            errorMessage = Some(
              s"""|Excel header does not conform to the schema.
                  | Header: ${columnNames.mkString(", ")}
                  | Schema: ${fieldNames.mkString(", ")}
                  |Expected: ${fieldNames(i)} but found: ${columnNames(i)}
                  |$source""".stripMargin
            )
          }
          i += 1
        }
      } else {
        errorMessage = Some(
          s"""|Number of column in Excel header is not equal to number of fields in the schema:
              | Header length: $headerLen, schema size: $schemaSize
              |$source""".stripMargin
        )
      }

      errorMessage.foreach { msg =>
        if (enforceSchema) {
          logWarning(msg)
        } else {
          throw new IllegalArgumentException(msg)
        }
      }
    }
  }

}
