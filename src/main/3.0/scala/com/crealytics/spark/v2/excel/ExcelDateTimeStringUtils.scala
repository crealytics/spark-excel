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

package com.crealytics.spark.v2.excel

import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.catalyst.util._
import java.time.ZoneId
import org.apache.spark.sql.catalyst.util.TimestampFormatter
import org.apache.spark.sql.catalyst.util.LegacyDateFormats.FAST_DATE_FORMAT

/** Wrapping the API change between spark 3.0 vs 3.1 */
object ExcelDateTimeStringUtils {
  def stringToTimestamp(v: String, zoneId: ZoneId): Option[Long] = {
    val str = UTF8String.fromString(DateTimeUtils.cleanLegacyTimestampStr(v))
    DateTimeUtils.stringToTimestamp(str, zoneId)
  }

  def stringToDate(v: String, zoneId: ZoneId): Option[Int] = {
    val str = UTF8String.fromString(DateTimeUtils.cleanLegacyTimestampStr(v))
    DateTimeUtils.stringToDate(str, zoneId)
  }

  def getTimestampFormatter(options: ExcelOptions): TimestampFormatter = TimestampFormatter(
    options.timestampFormat,
    options.zoneId,
    options.locale,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = true
  )

  def getDateFormatter(options: ExcelOptions): DateFormatter =
    DateFormatter(options.dateFormat, options.zoneId, options.locale, legacyFormat = FAST_DATE_FORMAT, isParsing = true)
}
