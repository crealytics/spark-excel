package com.crealytics.spark.excel

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Locale, TimeZone}

import org.apache.commons.lang.time.FastDateFormat
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils, ParseMode, PermissiveMode}

class ExcelOptions(@transient val parameters: CaseInsensitiveMap[String]) extends Logging with Serializable {

  def this(parameters: Map[String, String]) = {
    this(CaseInsensitiveMap(parameters))
  }

  private def getInt(paramName: String, default: Int): Int = {
    val paramValue = parameters.get(paramName)
    paramValue match {
      case None => default
      case Some(null) => default
      case Some(value) =>
        try {
          value.toInt
        } catch {
          case e: NumberFormatException =>
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

  val columnPruning = true
  val treatEmptyValuesAsNulls = getBool("treatEmptyValuesAsNulls", false)
  val addColorColumns = getBool("addColorColumns", false)
  val excerptSize = getInt("excerptSize", 10)

  val parseMode: ParseMode =
    parameters.get("mode").map(ParseMode.fromString).getOrElse(PermissiveMode)

  val headerFlag = getBool("header")
  val inferSchemaFlag = getBool("inferSchema")
  val ignoreLeadingWhiteSpaceInRead = getBool("ignoreLeadingWhiteSpace", default = false)
  val ignoreTrailingWhiteSpaceInRead = getBool("ignoreTrailingWhiteSpace", default = false)

  // For write, both options were `true` by default. We leave it as `true` for
  // backwards compatibility.
  val ignoreLeadingWhiteSpaceFlagInWrite = getBool("ignoreLeadingWhiteSpace", default = true)
  val ignoreTrailingWhiteSpaceFlagInWrite = getBool("ignoreTrailingWhiteSpace", default = true)

  val columnNameOfCorruptRecord =
    parameters.getOrElse("columnNameOfCorruptRecord", "") // TODO

  val nullValue = parameters.getOrElse("nullValue", "")

  val timeZone: TimeZone =
    DateTimeUtils.getTimeZone(parameters.getOrElse(DateTimeUtils.TIMEZONE_OPTION, "")) // TODO

  // Uses `FastDateFormat` which can be direct replacement for `SimpleDateFormat` and thread-safe.
  val dateFormat: FastDateFormat =
    FastDateFormat.getInstance(parameters.getOrElse("dateFormat", "yyyy-MM-dd"), Locale.US)

  val timestampFormat: Option[String] = parameters.get("timestampFormat")
  val timestampParser: String => Timestamp =
    timestampFormat
      .map { fmt =>
        val parser = new SimpleDateFormat(fmt)
        (stringValue: String) => new Timestamp(parser.parse(stringValue).getTime)
      }
      .getOrElse((stringValue: String) => Timestamp.valueOf(stringValue))

  val samplingRatio =
    parameters.get("samplingRatio").map(_.toDouble).getOrElse(1.0)

  /**
    * Forcibly apply the specified or inferred schema to datasource files.
    * If the option is enabled, headers of CSV files will be ignored.
    */
  val enforceSchema = getBool("enforceSchema", default = true)

  /**
    * String representation of an empty value in read and in write.
    */
  val emptyValue = parameters.get("emptyValue")

  /**
    * The string is returned when CSV reader doesn't have any characters for input value,
    * or an empty quoted string `""`. Default value is empty string.
    */
  val emptyValueInRead = emptyValue.getOrElse("")

  /**
    * The value is used instead of an empty string in write. Default value is `""`
    */
  val emptyValueInWrite = emptyValue.getOrElse("\"\"")
}
