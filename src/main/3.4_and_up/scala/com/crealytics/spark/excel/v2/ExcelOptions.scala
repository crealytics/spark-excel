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

import org.apache.spark.sql.catalyst.FileSourceOptions
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.internal.SQLConf

class ExcelOptions(
  @transient
  val parameters: CaseInsensitiveMap[String],
  val defaultTimeZoneId: String,
  val defaultColumnNameOfCorruptRecord: String
) extends FileSourceOptions(parameters)
    with ExcelOptionsTrait {

  def this(parameters: Map[String, String], defaultTimeZoneId: String) = {
    this(CaseInsensitiveMap(parameters), defaultTimeZoneId, SQLConf.get.columnNameOfCorruptRecord)
  }

  def this(parameters: Map[String, String], defaultTimeZoneId: String, defaultColumnNameOfCorruptRecord: String) = {
    this(CaseInsensitiveMap(parameters), defaultTimeZoneId, defaultColumnNameOfCorruptRecord)
  }

}
