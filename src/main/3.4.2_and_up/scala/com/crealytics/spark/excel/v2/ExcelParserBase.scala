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

import _root_.org.apache.spark.sql.catalyst.util.BadRecordException
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.catalyst.InternalRow

trait ExcelParserBase {

  protected def getCurrentInput: UTF8String
  def badRecord(partialResults: Array[InternalRow], baseException: Throwable): BadRecordException =
    BadRecordException(() => getCurrentInput, () => partialResults, baseException)
}
