package org.apache.flink.stats;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple4;

public class StringStatsTuple extends Tuple4<Integer, String, String, Long> {

  private static final long serialVersionUID = 1L;
  public static final int COL_INDEX_POS = 0;
  public static final int STRING_VALUE_POS = 1;
  public static final int PATTERN_POS = 2;
  public static final int COUNTER_POS = 3;

  @Deprecated
  @SuppressWarnings({ "squid:MissingDeprecatedCheck", "squid:S1133" })
  public StringStatsTuple() {
    // required by Flink
  }

  public StringStatsTuple(int colIndex, String value) {
    super(colIndex, value, generatePattern(value), 1L);
  }

  private static String generatePattern(String value) {
    StringBuilder ret = new StringBuilder();
    value = StringUtils.stripAccents(value);
    for (int i = 0; i < value.length(); i++) {
      if (value.charAt(i) >= 'a' && value.charAt(i) <= 'z') {
        ret.append("a");
      } else if (value.charAt(i) >= 'A' && value.charAt(i) <= 'Z') {
        ret.append("A");
      } else if (value.charAt(i) >= '0' && value.charAt(i) <= '9') {
        ret.append("#");
      } else if (value.charAt(i) == ' ' || value.charAt(i) == '\t') {
        ret.append("b");
      } else if (value.charAt(i) == '#') {
        ret.append("#");
      } else if (value.charAt(i) != '/' && value.charAt(i) != ':' && value.charAt(i) != '.'
          && value.charAt(i) != '-' && value.charAt(i) != '\'') {
        ret.append("?");
      } else {
        ret.append(value.charAt(i));
      }
    }
    return ret.toString();
  }

  // GETTERS and SETTERS
  public Integer getColumnIndex() {
    return getField(COL_INDEX_POS);
  }

  public void setColumnIndex(Integer columnIndex) {
    setField(columnIndex, COL_INDEX_POS);
  }

  public String getValue() {
    return getField(STRING_VALUE_POS);
  }

  public void setValue(String value) {
    setField(value, STRING_VALUE_POS);
  }

  public String getPattern() {
    return getField(PATTERN_POS);
  }

  public void setPattern(String pattern) {
    setField(pattern, PATTERN_POS);
  }

  public long getCount() {
    return getField(COUNTER_POS);
  }

  public void setCount(long count) {
    setField(count, COUNTER_POS);
  }
}
