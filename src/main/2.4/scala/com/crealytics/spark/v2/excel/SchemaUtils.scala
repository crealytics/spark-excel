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

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.types.StructType

/** Utils for handling schemas. (Copied from spark.util)
  */
object SchemaUtils {

  /** Checks if an input schema has duplicate column names. This throws an exception if the
    * duplication exists.
    *
    * @param schema schema to check
    * @param colType column type name, used in an exception message
    * @param caseSensitiveAnalysis whether duplication checks should be case sensitive or not
    */
  def checkSchemaColumnNameDuplication(
    schema: StructType,
    colType: String,
    caseSensitiveAnalysis: Boolean = false
  ): Unit = { checkColumnNameDuplication(schema.map(_.name), colType, caseSensitiveAnalysis) }

  // Returns true if a given resolver is case-sensitive
  private def isCaseSensitiveAnalysis(resolver: Resolver): Boolean = {
    if (resolver == caseSensitiveResolution) { true }
    else if (resolver == caseInsensitiveResolution) { false }
    else {
      sys.error(
        "A resolver to check if two identifiers are equal must be " +
          "`caseSensitiveResolution` or `caseInsensitiveResolution` in o.a.s.sql.catalyst."
      )
    }
  }

  /** Checks if input column names have duplicate identifiers. This throws an exception if
    * the duplication exists.
    *
    * @param columnNames column names to check
    * @param colType column type name, used in an exception message
    * @param resolver resolver used to determine if two identifiers are equal
    */
  def checkColumnNameDuplication(columnNames: Seq[String], colType: String, resolver: Resolver): Unit = {
    checkColumnNameDuplication(columnNames, colType, isCaseSensitiveAnalysis(resolver))
  }

  /** Checks if input column names have duplicate identifiers. This throws an exception if
    * the duplication exists.
    *
    * @param columnNames column names to check
    * @param colType column type name, used in an exception message
    * @param caseSensitiveAnalysis whether duplication checks should be case sensitive or not
    */
  def checkColumnNameDuplication(columnNames: Seq[String], colType: String, caseSensitiveAnalysis: Boolean): Unit = {
    val names = if (caseSensitiveAnalysis) columnNames else columnNames.map(_.toLowerCase)
    if (names.distinct.length != names.length) {
      val duplicateColumns = names
        .groupBy(identity)
        .collect { case (x, ys) if ys.length > 1 => s"`$x`" }
      throw new RuntimeException(s"Found duplicate column(s) $colType: ${duplicateColumns.mkString(", ")}")
    }
  }
}
