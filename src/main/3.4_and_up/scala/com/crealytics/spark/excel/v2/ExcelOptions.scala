/*
 * Copyright 2022 Martin Mauch (@nightscape)
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

import org.apache.spark.sql.catalyst.{DataSourceOptions, FileSourceOptions}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.catalyst.util.DateFormatter
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.ParseMode
import org.apache.spark.sql.catalyst.util.PermissiveMode
import org.apache.spark.sql.catalyst.util.TimestampFormatter
import org.apache.spark.sql.internal.SQLConf

import java.time.ZoneId
import java.util.Locale
import scala.annotation.nowarn

class ExcelOptions(
  @transient
  val parameters: CaseInsensitiveMap[String],
  defaultTimeZoneId: String,
  defaultColumnNameOfCorruptRecord: String
) extends FileSourceOptions(parameters)  {

  import ExcelOptions._

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
        try { Some(value.toInt) }
        catch {
          case _: NumberFormatException =>
            throw new RuntimeException(s"$paramName should be an integer. Found $value")
        }
    }
  }

  private def getBool(paramName: String, default: Boolean): Boolean = {
    val param = parameters.getOrElse(paramName, default.toString)
    if (param == null) { default }
    else if (param.toLowerCase(Locale.ROOT) == "true") { true }
    else if (param.toLowerCase(Locale.ROOT) == "false") { false }
    else { throw new Exception(s"$paramName flag can be true or false") }
  }

  /* Parsing mode, how to handle corrupted record. Default to permissive */
  val parseMode: ParseMode = parameters
    .get(MODE)
    .map(ParseMode.fromString)
    .getOrElse(PermissiveMode)

  val zoneId: ZoneId = ZoneId
    .of(parameters.getOrElse(DateTimeUtils.TIMEZONE_OPTION, defaultTimeZoneId))

  /* A language tag in IETF BCP 47 format */
  val locale: Locale = parameters.get(LOCALE).map(Locale.forLanguageTag).getOrElse(Locale.US)

  val dateFormat: String = parameters.getOrElse(DATEFORMAT, DateFormatter.defaultPattern)

  @nowarn
  val timestampFormat: String = parameters.getOrElse(TIMESTAMPFORMAT, TimestampFormatter.defaultPattern)

  /* Have header line when reading and writing */
  val header = getBool(HEADER, default = true)

  /* Number of rows to ignore after header. Only in reading */
  val ignoreAfterHeader = getInt(IGNOREAFTERHEADER).getOrElse(0)

  val inferSchema = getBool(INFERSCHEMA, default = false)
  val excerptSize = getInt(EXCERPTSIZE)

  /** Forcibly apply the specified or inferred schema to data files. If the option is enabled, headers of ABC files will
    * be ignored.
    */
  val enforceSchema = getBool(ENFORCESCHEMA, default = true)

  /* Name for column of corrupted records */
  val columnNameOfCorruptRecord = parameters
    .getOrElse(COLUMNNAMEOFCORRUPTRECORD, defaultColumnNameOfCorruptRecord)

  val nullValue = parameters.getOrElse(NULLVALUE, "")
  val nanValue = parameters.getOrElse(NANVALUE, "NaN")
  val positiveInf = parameters.getOrElse(POSITIVEINF, "Inf")
  val negativeInf = parameters.getOrElse(NEGATIVEINF, "-Inf")

  /* If true, format the cells without rounding and scientific notations */
  val usePlainNumberFormat = getBool(USEPLAINNUMBERFORMAT, default = false)

  /* If true, keep undefined (Excel) rows */
  val keepUndefinedRows = getBool(KEEPUNDEFINEDROWS, default = false)

  /* Use null value for error cells */
  val useNullForErrorCells = getBool(USENULLFORERRORCELLS, default = false)

  /* Additional column for color */
  val addColorColumns = getBool(ADDCOLORCOLUMNS, default = false)
  val ignoreLeadingWhiteSpace = getBool(IGNORELEADINGWHITESPACE, default = false)
  val ignoreTrailingWhiteSpace = getBool(IGNORETRAILINGWHITESPACE, default = false)

  /* Additional column for excel row number */
  val columnNameOfRowNumber = parameters.get(COLUMNNAMEOFROWNUMBER)

  /* Data address, default to everything */
  val dataAddress = parameters.getOrElse(DATAADDRESS, "A1")

  /* Workbook password, optional */
  val workbookPassword = parameters.get(WORKBOOKPASSWORD)

  /* Output excel file extension, default to xlsx */
  val fileExtension = parameters.get(FILEEXTENSION) match {
    case Some(value) => value.trim
    case None => "xlsx"
  }

  /* Defines fraction of file used for schema inferring. For default and
     invalid values, 1.0 will be used */
  val samplingRatio = {
    val r = parameters.get(SAMPLINGRATIO).map(_.toDouble).getOrElse(1.0)
    if (r > 1.0 || r <= 0.0) 1.0 else r
  }

  /** Optional parameter for using a streaming reader which can help with big files (will fail if used with xls format
    * files)
    */
  val maxRowsInMemory = getInt(MAXROWSINMEMORY)

  // scalastyle:off
  /** Optional parameter for <a
    * href="https://poi.apache.org/apidocs/5.0/org/apache/poi/util/IOUtils.html#setByteArrayMaxOverride-int-">maxByteArraySize</a>
    */
  val maxByteArraySize = getInt(MAXBYTEARRAYSIZE)

  // scalastyle:on
  /** Optional parameter for specifying the number of bytes at which a zip entry is regarded as too large for holding in
    * memory and the data is put in a temp file instead - useful for sheets with a lot of data
    */
  val tempFileThreshold = getInt(TEMPFILETHRESHOLD)
}



object ExcelOptions extends DataSourceOptions {
  val MODE = newOption("mode")
  val LOCALE = newOption("locale")
  val DATEFORMAT = newOption("dateFormat")
  val TIMESTAMPFORMAT = newOption("timestampFormat")
  val HEADER = newOption("header")
  val INFERSCHEMA = newOption("inferSchema")
  val EXCERPTSIZE = newOption("excerptSize")
  val IGNOREAFTERHEADER = newOption("ignoreAfterHeader")
  val ENFORCESCHEMA = newOption("enforceSchema")
  val COLUMNNAMEOFCORRUPTRECORD = newOption("columnNameOfCorruptRecord")
  val NULLVALUE = newOption("nullValue")
  val NANVALUE = newOption("nanValue")
  val POSITIVEINF = newOption("positiveInf")
  val NEGATIVEINF = newOption("negativeInf")
  val USEPLAINNUMBERFORMAT = newOption("usePlainNumberFormat")
  val KEEPUNDEFINEDROWS = newOption("keepUndefinedRows")
  val USENULLFORERRORCELLS = newOption("useNullForErrorCells")
  val ADDCOLORCOLUMNS = newOption("addColorColumns")
  val IGNORELEADINGWHITESPACE = newOption("ignoreLeadingWhiteSpace")
  val IGNORETRAILINGWHITESPACE = newOption("ignoreTrailingWhiteSpace")
  val COLUMNNAMEOFROWNUMBER = newOption("columnNameOfRowNumber")
  val DATAADDRESS = newOption("dataAddress")
  val WORKBOOKPASSWORD = newOption("workbookPassword")
  val FILEEXTENSION = newOption("fileExtension")
  val MAXROWSINMEMORY = newOption("maxRowsInMemory")
  val SAMPLINGRATIO = newOption("samplingRatio")
  val MAXBYTEARRAYSIZE = newOption("maxByteArraySize")
  val TEMPFILETHRESHOLD = newOption("tempFileThreshold")

}