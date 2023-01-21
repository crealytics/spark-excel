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

package com.crealytics.spark.excel.v2

import java.io.Closeable

case class SheetData[T](rowIterator: Iterator[T], resourcesToClose: Seq[Closeable] = Seq.empty) extends Closeable {
  def modifyIterator(f: Iterator[T] => Iterator[T]): SheetData[T] = SheetData(f(rowIterator), resourcesToClose)
  def append(other: SheetData[T]): SheetData[T] =
    SheetData(rowIterator ++ other.rowIterator, resourcesToClose ++ other.resourcesToClose)
  override def close(): Unit = resourcesToClose.foreach(_.close())
}
