package org.apache.flink.stats;

import java.io.Serializable;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.java.tuple.Tuple2;

public class StatsPojo implements Serializable {

	private static final long serialVersionUID = 1L;

	public static final String TYPE_BASIC = "basic";
	public static final String TYPE_WORD = "word";
	public static final String TYPE_PATTERN = "pattern";

	private int columnIndex;
	private String statsType = TYPE_BASIC;
	private long rowCount;
	private long numericValues;
	private long nullValues;
	private long emptyString;
	private Double min = Double.NaN;
	private Double max = Double.NaN;

	private double sum;
	private double unormalizedVariance;// un-normalized variance
	private double skew;
	private double kurtosis;
	private Map<String, Long> topValues;// only if statsType == TYPE_WORD
	private Map<String, Long> topPatterns;// only if statsType == TYPE_PATTERN

	private long minLength;
	private long maxLength;
	private long sumLength;

	// possible types for string fields
	private long booleanValues;
	private long intValues;
	private long longValues;
	private long floatValues;
	private long doubleValues;
	private long dateValues;

	// mode -- too expansive (use topK)
	// distinct values -- sort and use top
	// median -- too expansive, use approx. quartiles
	// approx. quartiles?

	public StatsPojo() {
	}

	public StatsPojo(int columnIndex, String statsType) {
		this(columnIndex);
		this.statsType = statsType;
	}

	public StatsPojo(int columnIndex) {
		this.columnIndex = columnIndex;
	}

	public double getVariancePopulation() {
		return unormalizedVariance / numericValues;
	}

	public Double getStdDevPopulation() {
		return Math.sqrt(getVariancePopulation());
	}

	public double getVarianceSample() {
		return unormalizedVariance / (numericValues - 1);
	}

	public Double getStdDevSample() {
		return Math.sqrt(getVarianceSample());
	}

	public Double getMean() {
		return sum / numericValues;
	}

	public Double getMeanSquareError() {
		return getStdDevSample() / Math.sqrt(numericValues);
	}

	public Double getPopulationSkewness() {
		return skew * Math.sqrt(numericValues) / Math.pow(unormalizedVariance, 3.0 / 2.0);
	}

	public Double getSampleSkewness() {
		return skew * Math.sqrt(numericValues - 1.0) / Math.pow(unormalizedVariance, 3.0 / 2.0);
	}

	public Double getPopulationKurtosis() {
		return (kurtosis * numericValues) / Math.pow(unormalizedVariance, 2.0);
	}

	public Double getSampleKurtosis() {
		return (kurtosis * (numericValues - 1.0)) / Math.pow(unormalizedVariance, 2.0);
	}

	public Double getExcessPopulationKurtosis() {
		return getPopulationKurtosis() - 3.0;
	}

	public Double getExcessSampleKurtosis() {
		return getSampleKurtosis() - 3.0;
	}

	/**
	 * Returns the average length.
	 * 
	 * @return the average length
	 */
	public Double getAvgLength() {
		if (getNonNullValues() == 0) {
			return 0.0;
		}
		return (double) sumLength / getNonNullValues();
	}

	private long getNonNullValues() {
		return rowCount - nullValues;
	}

	/**
	 * Update the state of this statistics object.
	 * 
	 * @param val a new value
	 * @return the Double value (if any) of the passed val
	 */
	public Double updateStats(Object val) {
		this.rowCount++;
		this.nullValues = val == null ? nullValues + 1 : nullValues;
		updateTypeStats(val);
		final Double doubleVal = CastUtils.getDoubleVal(val);
		if (doubleVal != null) {
			this.numericValues++;
			this.min = min.isNaN() ? doubleVal : Math.min(min, doubleVal);
			this.max = max.isNaN() ? doubleVal : Math.max(max, doubleVal);
		}
		long valLength = getValLength(val);
		this.minLength = Math.min(minLength, valLength);
		this.maxLength = Math.max(maxLength, valLength);
		this.sumLength += valLength;
		if (val instanceof String && ((String) val).trim().isEmpty()) {
			this.emptyString++;
		}
		return doubleVal;
	}

	private void updateTypeStats(Object val) {
		if (!(val instanceof String) || ((String) val).trim().isEmpty()) {
			return;
		}
		final String strVal = (String) val;
		checkBooleanType(strVal);
		checkIntType(strVal);
		checkLongType(strVal);
		checkFloatType(strVal);
		checkDoubleType(strVal);
		checkDateType(strVal);
		// TODO check other types
	}

	private void checkBooleanType(final String strVal) {
		if (Boolean.parseBoolean(strVal)) {
			booleanValues++;
		}
	}

	private void checkIntType(final String strVal) {
		try {
			Integer.parseInt(strVal);
			intValues++;
		} catch (NumberFormatException ex) {
			// ignore, do nothing
		}
	}

	private void checkLongType(final String strVal) {
		try {
			Long.parseLong(strVal);
			longValues++;
		} catch (NumberFormatException ex) {
			// ignore, do nothing
		}
	}

	private void checkFloatType(final String strVal) {
		try {
			Float.parseFloat(strVal);
			floatValues++;
		} catch (NumberFormatException ex) {
			// ignore, do nothing
		}
	}

	private void checkDoubleType(final String strVal) {
		try {
			Double.parseDouble(strVal);
			doubleValues++;
		} catch (NumberFormatException ex) {
			// ignore, do nothing
		}
	}

	private void checkDateType(final String strVal) {
		try {
			final Locale loc = null;
			final String datePattern = null;
			parseDate(loc, strVal, datePattern);
			dateValues++;
		} catch (Exception ex) {
			// ignore, do nothing
		}
	}

	private static final String DEFAULT_DATE_PATTERN_OUT = "yyyy-MM-dd";
	private static final String[] DEFAULT_DATE_PATTERN_IN = { //
			DEFAULT_DATE_PATTERN_OUT, //
			"yyyyMMdd", //
			"yyyy/MM/dd", //
			"dd/MM/yyyy", //
			"dd-MM-yyyy", //
			"MM/dd/yyyy", //
			"MM-dd-yyyy" };

	/**
	 * Return the date object of the passed string.
	 * 
	 * @param loc         the Locale
	 * @param str         the string version of the date
	 * @param datePattern the date pattern (if null it will use the default set of
	 *                    pattern)
	 * @return the corresponding date object
	 * @throws ParseException if the date string is invalid with respect to the
	 *                        passed pattern
	 */
	public static java.sql.Date parseDate(Locale loc, String str, String datePattern) throws ParseException {
		if (datePattern == null) {
			return new java.sql.Date(DateUtils.parseDateStrictly(str, loc, DEFAULT_DATE_PATTERN_IN).getTime());
		} else {
			return new java.sql.Date(DateUtils.parseDateStrictly(str, loc, datePattern).getTime());
		}
	}

	// from calcite SqlFunctions.java
	private long getValLength(Object val) {
		if (val == null) {
			return 0;
		}
		if (val instanceof Character) {
			return 1;
		}
		if (val instanceof String) {
			return ((String) val).length();
		}
		if (val instanceof Integer) {
			return ((Integer) val).toString().length();
		}
		if (val instanceof Long) {
			return ((Long) val).toString().length();
		}
		if (val instanceof Boolean) {
			return ((Boolean) val) ? 4 : 5;
		}
		if (val instanceof Double) {
			return CastUtils.getDoubleAsStr(val).length();
		}
		if (val instanceof Float) {
			return CastUtils.getFloatAsStr((Float) val).length();
		}
		if (val instanceof BigDecimal) {
			return CastUtils.getBigDecimalAsStr((BigDecimal) val).length();
		}
		throw new IllegalArgumentException("Class " + val.getClass().getCanonicalName() + " not handled yet");
	}

	/**
	 * Print the statistics of this column.
	 * 
	 * @param columnName the column name
	 * @param k          the top size
	 * @return the base statistics statistics
	 */
	public String toString(String columnName, int k) {
		final StringBuilder ret = new StringBuilder();
		ret.append("\n----------------------------------------");
		ret.append("\n Statistics of column[").append(columnIndex).append("]: '").append(columnName).append("'");
		ret.append("\n----------------------------------------");
		ret.append("\nRow count: " + getRowCount());
		ret.append("\nNumeric values: " + getNullValues());
		ret.append("\nNull values: " + getNullValues());
		ret.append("\nEmpty strings: " + getEmptyString());
		ret.append(String.format("%nMean: %.3f", getMean()));
		ret.append(String.format("%nMean square error (MSE): %.3f", getMeanSquareError()));
		ret.append(String.format("%nVariance (population): %.3f", getVariancePopulation()));
		ret.append(String.format("%nStdDev (population): %.3f", getStdDevPopulation()));
		ret.append(String.format("%nVariance (sample): %.3f", getVarianceSample()));
		ret.append(String.format("%nStdDev (sample): %.3f", getStdDevSample()));
		ret.append("\nMin: " + getMin());
		ret.append("\nMax: " + getMax());
		ret.append("\nMinLength: " + getMinLength());
		ret.append("\nMaxLength: " + getMaxLength());
		ret.append(String.format("%nAvgLength: %.3f", getAvgLength()));
		ret.append(String.format("%nSkewness (population): %.3f", getPopulationSkewness()));
		ret.append(String.format("%nSkewness (sample): %.3f", getSampleSkewness()));
		ret.append(String.format("%nKurtosis (population): %.3f", getPopulationKurtosis()));
		ret.append(String.format("%nKurtosis (sample): %.3f", getSampleKurtosis()));

		// if excess == 0 --> Mesokurtic distibution: Normal and Binomial distributions
		// if excess > 0 --> Leptokurtik distibution (aka super-Gaussian distributions):
		// ..... distribution has "fatter tails" --> Student't, Rayleigh, Laplace,
		// ............ Exponential, Poisson, Logistic
		// ..... REMARK: usually there are outliers so check the data...
		// if excess < 0 --> Platykurtic distibution (aka sub-Gaussian distributions)
		// ....... distribution has "thinner tails" --> Continuous, Uniform, Bernoulli
		ret.append(String.format("%nKurtosis (population ex): %.3f", getExcessPopulationKurtosis()));
		ret.append(String.format("%nKurtosis (sample ex): %.3f", getExcessSampleKurtosis()));

		if (topValues != null) {
			ret.append("\n------ STATS for string column only");
			ret.append("\n------ TYPE STATS:");
			ret.append("\n\tboolean values: " + booleanValues + "");
			ret.append("\n\tint values: " + intValues);
			ret.append("\n\tlong values: " + longValues);
			ret.append("\n\tfloat values: " + floatValues);
			ret.append("\n\tdouble values: " + doubleValues);
			ret.append("\n\tdate values: " + dateValues);
			ret.append("\n------ TOP-" + k + " VALUES:");
			topValues.entrySet()//
					.stream()//
					.sorted((x, y) -> y.getValue().compareTo(x.getValue()))//
					.forEach(x -> ret.append("\n\t").append(x.getKey() + " => " + x.getValue()));
			ret.append("\n------ TOP-" + k + " PATTERNS:");
			topPatterns.entrySet()//
					.stream()//
					.sorted((x, y) -> y.getValue().compareTo(x.getValue()))//
					.forEach(x -> ret.append("\n\t").append(x.getKey() + " => " + x.getValue()));
		}
		return ret.toString();

	}

	/**
	 * Reduce this and another StatsPojo.
	 * 
	 * @param other the other StatsPojo
	 * @return the reduced version of this tuple.
	 */
	public StatsPojo reduce(StatsPojo other) {
		final double s1 = getUnormalizedVariance();
		final double s2 = other.getUnormalizedVariance();
		final long m = getNumericValues();
		final long n = other.getNumericValues();
		final long nPlusM = n + m;
		final double t1 = getSum();
		final double t2 = other.getSum();
		// skewness vars
		final double skew1 = getSkew();
		final double skew2 = other.getSkew();
		final double delta = (t2 / n) - (t1 / m);
		// kurtosis vars
		final double kurt1 = getKurtosis();
		final double kurt2 = other.getKurtosis();

		if (getNumericValues() == 0L || other.getNumericValues() == 0L) {
			setUnormalizedVariance(other.getNumericValues() == 0L ? s1 : s2);
		} else {

			// Philippe Pebay version
			setUnormalizedVariance(s1 + s2 + (Math.pow(delta, 2.0) * (n * m)) / nPlusM);
			/// Test Pairwise version
			// setUnormalizedVariance(s1 + s2 + (((double) m / (n * nPlusM)) * Math.pow(((
			/// (double) n / m) * t1) - t2, 2.0)));
			setSkew(skew1 + skew2 + //
					(Math.pow(delta, 3.0) * (n * m) * (m - n) / Math.pow(nPlusM, 2.0)) + //
					(3 * delta * (m * s2 - n * s1) / nPlusM) //
			);
			setKurtosis(kurt1 + kurt2 + //
					(Math.pow(delta, 4.0) * (n * m) * (Math.pow(m, 2.0) - (n * m) + Math.pow(n, 2.0))
							/ Math.pow(nPlusM, 3.0))
					+ (6.0 * Math.pow(delta, 2.0) * ((Math.pow(m, 2.0) * s2) + (Math.pow(n, 2.0) * s1))
							/ Math.pow(nPlusM, 2.0))
					+ (4.0 * delta * ((m * skew2) - (n * skew1)) / nPlusM));
		}
		setRowCount(getRowCount() + other.getRowCount());
		setNumericValues(nPlusM);
		setNullValues(getNullValues() + other.getNullValues());
		setEmptyString(getEmptyString() + other.getEmptyString());
		setSum(t1 + t2);
		setMin(getMin().isNaN() ? other.getMin() : getMin2(getMin(), other.getMin()));
		setMax(getMax().isNaN() ? other.getMax() : getMax2(getMax(), other.getMax()));
		setMinLength(Math.min(getMinLength(), other.getMinLength()));
		setMaxLength(Math.max(getMaxLength(), other.getMaxLength()));
		setSumLength(getSumLength() + other.getSumLength());
		return this;
	}

	private double getMax2(Double currentMax, Double possibleMax) {
		return possibleMax.isNaN() ? currentMax : Math.max(currentMax, possibleMax);
	}

	private double getMin2(Double currentMin, Double possibleMin) {
		return possibleMin.isNaN() ? currentMin : Math.min(currentMin, possibleMin);
	}

	// ---------------------------------------------------------
	public long getNumericValues() {
		return numericValues;
	}

	public StatsPojo setNumericValues(long numericValues) {
		this.numericValues = numericValues;
		return this;
	}

	public long getNullValues() {
		return nullValues;
	}

	public StatsPojo setNullValues(long nullValues) {
		this.nullValues = nullValues;
		return this;
	}

	public double getSum() {
		return sum;
	}

	public StatsPojo setSum(double sum) {
		this.sum = sum;
		return this;
	}

	public double getUnormalizedVariance() {
		return unormalizedVariance;
	}

	public StatsPojo setUnormalizedVariance(double unormalizedVariance) {
		this.unormalizedVariance = unormalizedVariance;
		return this;
	}

	public Double getMin() {
		return min;
	}

	public StatsPojo setMin(Double min) {
		this.min = min;
		return this;
	}

	public Double getMax() {
		return max;
	}

	public StatsPojo setMax(Double max) {
		this.max = max;
		return this;
	}

	public long getMinLength() {
		return minLength;
	}

	public StatsPojo setMinLength(long minLength) {
		this.minLength = minLength;
		return this;
	}

	public long getMaxLength() {
		return maxLength;
	}

	public StatsPojo setMaxLength(long maxLength) {
		this.maxLength = maxLength;
		return this;
	}

	public long getSumLength() {
		return sumLength;
	}

	public StatsPojo setSumLength(long sumLength) {
		this.sumLength = sumLength;
		return this;
	}

	public long getEmptyString() {
		return emptyString;
	}

	public StatsPojo setEmptyString(long emptyString) {
		this.emptyString = emptyString;
		return this;
	}

	public long getRowCount() {
		return rowCount;
	}

	public StatsPojo setRowCount(long rowCount) {
		this.rowCount = rowCount;
		return this;
	}

	public long getBooleanValues() {
		return booleanValues;
	}

	public long getIntValues() {
		return intValues;
	}

	public long getLongValues() {
		return longValues;
	}

	public long getFloatValues() {
		return floatValues;
	}

	public long getDoubleValues() {
		return doubleValues;
	}

	public long getDateValues() {
		return dateValues;
	}

	public Map<String, Long> getTopValues() {
		return topValues;
	}

	public void setTopValues(Map<String, Long> topValues) {
		this.topValues = topValues;
	}

	public StatsPojo setTopValues(Iterable<Tuple2<String, Long>> it) {
		this.topValues = new HashMap<>();
		if (it != null) {
			for (Tuple2<String, Long> tuple : it) {
				this.topValues.put(tuple.f0, tuple.f1);
			}
		}
		return this;
	}

	public Map<String, Long> getTopPatterns() {
		return topPatterns;
	}

	public void setTopPatterns(Map<String, Long> topPatterns) {
		this.topPatterns = topPatterns;
	}

	public StatsPojo setTopPatterns(Iterable<Tuple2<String, Long>> it) {
		this.topPatterns = new HashMap<>();
		if (it != null) {
			for (Tuple2<String, Long> tuple : it) {
				this.topPatterns.put(tuple.f0, tuple.f1);
			}
		}
		return this;
	}

	public double getSkew() {
		return skew;
	}

	public void setSkew(double skew) {
		this.skew = skew;
	}

	public int getColumnIndex() {
		return columnIndex;
	}

	public void setColumnIndex(int columnIndex) {
		this.columnIndex = columnIndex;
	}

	public String getStatsType() {
		return statsType;
	}

	public void setStatsType(String statsType) {
		this.statsType = statsType;
	}

	public double getKurtosis() {
		return kurtosis;
	}

	public void setKurtosis(double kurtosis) {
		this.kurtosis = kurtosis;
	}

}