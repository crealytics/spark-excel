package com.crealytics.spark.v2.excel

import java.io.Closeable

case class CloseableIterator[T](iterator: Iterator[T], resourcesToClose: Seq[Closeable] = Seq.empty) extends Closeable {
  override def close(): Unit = {
    resourcesToClose.foreach(_.close())
  }
}
