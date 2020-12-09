package com.crealytics.spark.excel

import java.math.BigDecimal
import java.text.FieldPosition
import java.text.Format
import java.text.ParsePosition

/** A format that formats a double as a plain string without rounding and scientific notation.
  * All other operations are unsupported.
  * @see [[org.apache.poi.ss.usermodel.ExcelGeneralNumberFormat]] and SSNFormat from
  * [[org.apache.poi.ss.usermodel.DataFormatter]] from Apache POI.
  */
object PlainNumberFormat extends Format {

  override def format(number: AnyRef, toAppendTo: StringBuffer, pos: FieldPosition): StringBuffer =
    toAppendTo.append(new BigDecimal(number.toString).toPlainString)

  override def parseObject(source: String, pos: ParsePosition): AnyRef =
    throw new UnsupportedOperationException()
}
