package com.crealytics.spark.excel

trait ParameterExtractor {
    // Forces a Parameter to exist, otherwise an exception is thrown.
  def checkParameter(map: Map[String, String], param: String) = {
    if (!map.contains(param)) {
      throw new IllegalArgumentException(s"Parameter ${'"'}$param${'"'} is missing in options.")
    } else {
      map.apply(param)
    }
  }

  // Gets the Parameter if it exists, otherwise returns the default argument
  def parameterOrDefault(map: Map[String, String], param: String, default: String) =
    map.getOrElse(param, default)

}