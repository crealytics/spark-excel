package com.crealytics.spark.excel.utils

object Loan {
  def withCloseable[R <: AutoCloseable, T](closeable: R)(f: R => T): T =
    try f(closeable)
    finally if (closeable != null) closeable.close()
}
