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

package com.crealytics.spark.excel

import java.math.BigDecimal
import java.text.FieldPosition
import java.text.Format
import java.text.ParsePosition

/** A format that formats a double as a plain string without rounding and scientific notation. All other operations are
  * unsupported.
  * @see
  *   [[org.apache.poi.ss.usermodel.ExcelGeneralNumberFormat]] and SSNFormat from
  *   [[org.apache.poi.ss.usermodel.DataFormatter]] from Apache POI.
  */
object PlainNumberFormat extends Format {

  override def format(number: AnyRef, toAppendTo: StringBuffer, pos: FieldPosition): StringBuffer =
    toAppendTo.append(new BigDecimal(number.toString).toPlainString)

  override def parseObject(source: String, pos: ParsePosition): AnyRef =
    throw new UnsupportedOperationException()
}
