package org.apache.flink.stats;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Date;

public class CastUtils {

  protected static String getBigDecimalAsStr(BigDecimal x) {
    final String s = x.toString();
    if (s.startsWith("0")) {
      // we want ".1" not "0.1"
      return s.substring(1);
    } else if (s.startsWith("-0")) {
      // we want "-.1" not "-0.1"
      return ("-" + s.substring(2));
    } else {
      return s;
    }
  }

  protected static String getFloatAsStr(Float floatVal) {
    if (floatVal.isNaN()) {
      return "";
    }
    if (floatVal == 0) {
      return "0E0";
    }
    BigDecimal bigDecimal = new BigDecimal(floatVal, MathContext.DECIMAL32).stripTrailingZeros();
    final String s = bigDecimal.toString();
    return s.replaceAll("0*E", "E").replace("E+", "E");
  }

  protected static String getDoubleAsStr(Object val) {
    Double doubleVal = (Double) val;
    if (doubleVal.isNaN()) {
      return "";
    }
    if (doubleVal == 0) {
      return "0E0";
    }
    BigDecimal bigDecimal = new BigDecimal(doubleVal, MathContext.DECIMAL64).stripTrailingZeros();
    final String s = bigDecimal.toString();
    return s.replaceAll("0*E", "E").replace("E+", "E");
  }

  protected static Double getDoubleVal(Object val) {
    if (val == null || val instanceof String || val instanceof Boolean) {
      return null;
    }
    if (val instanceof Double) {
      return (Double) val;
    }
    if (val instanceof Integer) {
      return ((Integer) val).doubleValue();
    }
    if (val instanceof Long) {
      return ((Long) val).doubleValue();
    }
    if (val instanceof Date) {
      return (double) ((Date) val).getTime();
    }
    //
    throw new IllegalArgumentException("Class " + val.getClass() + " cannot be cast to Double");
  }

}
