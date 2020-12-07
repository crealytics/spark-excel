package com.crealytics.spark.excel;

import java.math.BigDecimal;
import java.text.FieldPosition;
import java.text.Format;
import java.text.ParsePosition;


/**
 * A format that formats a double as a plain string without rounding and scientific notation.
 * All other operations are unsupported.
 * @see <a href="https://github.com/apache/poi/blob/REL_4_1_2/src/java/org/apache/poi/ss/usermodel/ExcelGeneralNumberFormat.java">
 * ExcelGeneralNumberFormat</a> and <a href="https://github.com/apache/poi/blob/REL_4_1_2/src/java/org/apache/poi/ss/usermodel/DataFormatter.java#L1171">
 * SSNFormat</a> from Apache POI.
 **/
public class PlainNumberFormat extends Format {

    private static final long serialVersionUID = 1L;

    private static final Format INSTANCE = new PlainNumberFormat();

    private PlainNumberFormat() {
        // enforce singleton
    }

    public static Format getInstance() {
        return INSTANCE;
    }

    @Override
    public StringBuffer format(Object number, StringBuffer toAppendTo, FieldPosition pos) {
        return toAppendTo.append(new BigDecimal(number.toString()).toPlainString());
    }

    @Override
    public Object parseObject(String source, ParsePosition pos) {
        throw new UnsupportedOperationException();
    }

}
