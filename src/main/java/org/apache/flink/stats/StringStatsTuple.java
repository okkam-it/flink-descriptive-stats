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
	public StringStatsTuple() {
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

	// /**
	// * Convert to regex.
	// *
	// * @param value
	// * value to convert
	// * @return regex expression
	// */
	// public static String convertToRegex(String value) {
	// StringBuilder ret = new StringBuilder();
	// int prev = 0;
	// int minus = 0;
	// for (int i = 0; i < value.length(); i++) {
	// String currentPatt = String.valueOf(value.charAt(i));
	// if (prev == 0) {
	// ret.append(currentPatt);
	// ret.append("1");
	// } else {
	// if (currentPatt.equals(String.valueOf(ret.charAt(ret.length() - 2 - minus))))
	// {
	// int now = Integer.parseInt(String.valueOf(ret.charAt(ret.length() - 1)));
	// String newBuilder = ret.substring(0, ret.length() - 1);
	// if (minus == 1) {
	// now = Integer.parseInt(String.valueOf(ret.charAt(ret.length() - 1)))
	// + 10 * Integer.parseInt(String.valueOf(ret.charAt(ret.length() - 2)));
	// newBuilder = ret.substring(0, ret.length() - 2);
	// }
	// ret = new StringBuilder(newBuilder);
	// ret.append(String.valueOf(now + 1));
	// if (now + 1 >= 10) {
	// minus = 1;
	// } else {
	// minus = 0;
	// }
	// } else {
	// minus = 0;
	// ret.append(currentPatt);
	// ret.append("1");
	// }
	// }
	// prev = 1;
	// }
	//
	// StringBuilder next = new StringBuilder();
	//
	// for (int i = 0; i < ret.toString().length(); i += 2) {
	// if (ret.toString().charAt(i) == 'a') {
	// next.append("[a-z]");
	// } else if (ret.toString().charAt(i) == 'A') {
	// next.append("[A-Z]");
	// } else if (ret.toString().charAt(i) == '#') {
	// next.append("[0-9]");
	// } else if (ret.toString().charAt(i) == '?') {
	// next.append("[?]");
	// } else if (ret.toString().charAt(i) == 'b') {
	// next.append("[ ]");
	// } else {
	// next.append(ret.toString().charAt(i));
	// }
	// int n = 0;
	// for (int j = i + 1; j < ret.toString().length(); j++) {
	// if (ret.toString().charAt(j) <= '9' && ret.toString().charAt(j) >= '0') {
	// n++;
	// } else {
	// break;
	// }
	// }
	// if (n == 1) {
	// next.append("{" + ret.toString().charAt(i + 1) + "}");
	// } else if (n == 2) {
	// int x = 10 * Integer.parseInt(String.valueOf(ret.toString().charAt(i + 1)))
	// + Integer.parseInt(String.valueOf(ret.toString().charAt(i + 2)));
	// next.append("{" + x + "}");
	// i++;
	// }
	// }
	// return next.toString();
	// }

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
