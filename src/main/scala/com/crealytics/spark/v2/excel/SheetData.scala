package com.crealytics.spark.v2.excel

import java.io.Closeable

case class SheetData[T](rowIterator: Iterator[T], resourcesToClose: Seq[Closeable] = Seq.empty) extends Closeable {
  override def close(): Unit = {
    resourcesToClose.foreach(_.close())
  }
  def modifyIterator(f: Iterator[T] => Iterator[T]): SheetData[T] = SheetData(f(rowIterator), resourcesToClose)
  def append(other: SheetData[T]): SheetData[T] = SheetData(rowIterator ++ other.rowIterator, resourcesToClose ++ other.resourcesToClose)
}
