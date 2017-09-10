package com.crealytics.spark.excel.utils

object ParameterChecker {
  // Forces a Parameter to exist, otherwise an exception is thrown.
  def check(map: Map[String, String], param: String): String = {
    if (!map.contains(param)) {
      throw new IllegalArgumentException(s"Parameter ${'"'}$param${'"'} is missing in options.")
    } else {
      map.apply(param)
    }
  }
}
