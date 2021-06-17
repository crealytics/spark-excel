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

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.catalyst.util.DateFormatter
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.ParseMode
import org.apache.spark.sql.catalyst.util.PermissiveMode
import org.apache.spark.sql.internal.SQLConf

import java.time.ZoneId
import java.util.Locale

class ExcelOptions(
  @transient val parameters: CaseInsensitiveMap[String],
  defaultTimeZoneId: String,
  defaultColumnNameOfCorruptRecord: String
) extends Serializable {

  def this(parameters: Map[String, String], defaultTimeZoneId: String) = {
    this(CaseInsensitiveMap(parameters), defaultTimeZoneId, SQLConf.get.columnNameOfCorruptRecord)
  }

  def this(parameters: Map[String, String], defaultTimeZoneId: String, defaultColumnNameOfCorruptRecord: String) = {
    this(CaseInsensitiveMap(parameters), defaultTimeZoneId, defaultColumnNameOfCorruptRecord)
  }

  private def getInt(paramName: String): Option[Int] = {
    val paramValue = parameters.get(paramName)
    paramValue match {
      case None => None
      case Some(null) => None
      case Some(value) =>
        try {
          Some(value.toInt)
        } catch {
          case _: NumberFormatException =>
            throw new RuntimeException(s"$paramName should be an integer. Found $value")
        }
    }
  }

  private def getBool(paramName: String, default: Boolean = false): Boolean = {
    val param = parameters.getOrElse(paramName, default.toString)
    if (param == null) {
      default
    } else if (param.toLowerCase(Locale.ROOT) == "true") {
      true
    } else if (param.toLowerCase(Locale.ROOT) == "false") {
      false
    } else {
      throw new Exception(s"$paramName flag can be true or false")
    }
  }

  /* Parsing mode, how to handle corrupted record. Default to permissive*/
  val parseMode: ParseMode =
    parameters.get("mode").map(ParseMode.fromString).getOrElse(PermissiveMode)

  val zoneId: ZoneId = DateTimeUtils.getZoneId(parameters.getOrElse(DateTimeUtils.TIMEZONE_OPTION, defaultTimeZoneId))

  /* A language tag in IETF BCP 47 format*/
  val locale: Locale =
    parameters.get("locale").map(Locale.forLanguageTag).getOrElse(Locale.US)

  val dateFormat: String =
    parameters.getOrElse("dateFormat", DateFormatter.defaultPattern)

  val timestampFormat: String =
    parameters.getOrElse("timestampFormat", s"${DateFormatter.defaultPattern}'T'HH:mm:ss[.SSS][XXX]")

  val header = getBool("header")
  val inferSchema = getBool("inferSchema")
  val excerptSize = getInt("excerptSize")

  /** Forcibly apply the specified or inferred schema to datasource files.
    * If the option is enabled, headers of ABC files will be ignored.
    */
  val enforceSchema = getBool("enforceSchema", default = true)

  /* Name for column of corrupted records*/
  val columnNameOfCorruptRecord =
    parameters.getOrElse("columnNameOfCorruptRecord", defaultColumnNameOfCorruptRecord)

  val nullValue = parameters.getOrElse("nullValue", "")
  val nanValue = parameters.getOrElse("nanValue", "NaN")
  val positiveInf = parameters.getOrElse("positiveInf", "Inf")
  val negativeInf = parameters.getOrElse("negativeInf", "-Inf")

  /* Additional column for color*/
  val addColorColumns = getBool("addColorColumns", false)
  val ignoreLeadingWhiteSpace =
    getBool("ignoreLeadingWhiteSpace", default = false)
  val ignoreTrailingWhiteSpace =
    getBool("ignoreTrailingWhiteSpace", default = false)

  /* Data address, default to everything*/
  val dataAddress = parameters.getOrElse("dataAddress", "A1")

  /* Workbook password, optional*/
  val workbookPassword = parameters.get("workbookPassword")

}
