package org.apache.flink.stats;

import java.io.Serializable;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.java.tuple.Tuple2;

public class StatsPojo implements Serializable, Comparable<StatsPojo> {

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
  private Double avg = Double.NaN;// use bigdecimal to be more precise
  private Double min = Double.NaN;
  private Double max = Double.NaN;

  // private double sum;
  private double unormalizedVariance;// un-normalized variance
  private double skew;
  private double kurtosis;
  private Map<String, Integer> topValues;// only if statsType == TYPE_WORD
  private Map<String, Integer> topPatterns;// only if statsType == TYPE_PATTERN

  private Integer minLength;
  private Integer maxLength;
  private Double avgLength;// use bigdecimal to be more precise

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

  public StatsPojo(Object val, int columnIndex) {
    this.columnIndex = columnIndex;
    initStats(val);
  }

  private Double initStats(Object val) {
    this.rowCount++;
    this.nullValues = val == null ? nullValues + 1 : nullValues;
    updateTypeStats(val);
    final Double doubleVal = CastUtils.getDoubleVal(val);
    if (doubleVal != null) {
      this.numericValues++;
      this.min = doubleVal;
      this.max = doubleVal;
      this.avg = doubleVal;
    }
    final Integer valLength = getValLength(val);
    if (valLength != null) {
      this.minLength = valLength;
      this.maxLength = valLength;
      this.avgLength = valLength.doubleValue();
    }
    if (val instanceof String && ((String) val).trim().isEmpty()) {
      this.emptyString++;
    }
    return doubleVal;
  }

  /**
   * Reduce this and another StatsPojo.
   * 
   * @param other the other StatsPojo
   * @return the reduced version of this tuple.
   */
  public StatsPojo reduce(StatsPojo other) {
    // variance vars
    final double s1 = getUnormalizedVariance();
    final double s2 = other.getUnormalizedVariance();
    final long m = getNumericValues();
    final long n = other.getNumericValues();
    final long nPlusM = n + m;
    final long nTimesM = n * m;
    final double u1 = getAvg().isNaN() ? 0.0 : getAvg();
    final double u2 = other.getAvg().isNaN() ? 0.0 : other.getAvg();
    // skewness vars
    final double skew1 = getSkew();
    final double skew2 = other.getSkew();
    final double delta = u2 - u1;
    // kurtosis vars
    final double kurt1 = getKurtosis();
    final double kurt2 = other.getKurtosis();

    if (nPlusM == 0L) {
      setUnormalizedVariance(other.getNumericValues() == 0L ? s1 : s2);
    } else {

      // Philippe Pebay version
      setUnormalizedVariance(s1 + s2 + (Math.pow(delta, 2.0) * nTimesM) / nPlusM);
      /// Test Pairwise version
      // setUnormalizedVariance(s1 + s2 + (((double) m / (n * nPlusM)) * Math.pow(((
      /// (double) n / m) * t1) - t2, 2.0)));
      setSkew(skew1 + skew2 + //
          (Math.pow(delta, 3.0) * nTimesM * (m - n) / Math.pow(nPlusM, 2.0)) + //
          (3 * delta * (m * s2 - n * s1) / nPlusM) //
      );
      setKurtosis(kurt1 + kurt2 + //
          (Math.pow(delta, 4.0) * nTimesM * (Math.pow(m, 2.0) - nTimesM + Math.pow(n, 2.0))
              / Math.pow(nPlusM, 3.0))
          + (6.0 * Math.pow(delta, 2.0) * ((Math.pow(m, 2.0) * s2) + (Math.pow(n, 2.0) * s1))
              / Math.pow(nPlusM, 2.0))
          + (4.0 * delta * ((m * skew2) - (n * skew1)) / nPlusM));

      setMin(getMin().isNaN() ? other.getMin() : getMin2(getMin(), other.getMin()));
      setMax(getMax().isNaN() ? other.getMax() : getMax2(getMax(), other.getMax()));

      if (getAvg().isNaN() || other.getAvg().isNaN()) {
        setAvg(getAvg().isNaN() ? other.getAvg() : getAvg());
      } else {
        setAvg(u1 + n * (delta) / (nPlusM));
      }
    }
    setRowCount(getRowCount() + other.getRowCount());
    setNumericValues(nPlusM);
    setNullValues(getNullValues() + other.getNullValues());
    setEmptyString(getEmptyString() + other.getEmptyString());

    updateLengthStats(other);
    return this;
  }

  private void updateLengthStats(StatsPojo other) {
    if (getAvgLength() != null && other.getAvgLength() != null) {
      // both are not null
      final Double ul1 = getAvgLength();
      final Double ul2 = other.getAvgLength();
      final long nPlusM2 = other.getNonNullValues() + getNonNullValues();
      setMinLength(Math.min(getMinLength(), other.getMinLength()));
      setMaxLength(Math.max(getMaxLength(), other.getMaxLength()));
      setAvgLength(ul1 + other.getNonNullValues() * (ul2 - ul1) / nPlusM2);
    } else {
      setMinLength(getMinLength() != null ? getMinLength() : other.getMinLength());
      setMaxLength(getMaxLength() != null ? getMaxLength() : other.getMaxLength());
      setAvgLength(getAvgLength() != null ? getAvgLength() : other.getAvgLength());
    }
  }

  public double getPopulationVariance() {
    return unormalizedVariance / numericValues;
  }

  public double getSampleVariance() {
    return unormalizedVariance / (numericValues - 1);
  }

  public double getSampleVarianceStdError() {
    return getSampleVariance() * Math.sqrt(2.0 / (numericValues - 1.0));
  }

  public Double getPopulationStdDev() {
    return Math.sqrt(getPopulationVariance());
  }

  public Double getSampleStdDev() {
    return Math.sqrt(getSampleVariance());
  }

  public double getSampleStdDevStdError() {
    return getSampleStdDev() * (1.0 / Math.sqrt(2.0 * (numericValues - 1)));
  }

  public Double getAvg() {
    return avg;
  }

  private void setAvg(Double avg) {
    this.avg = avg;
  }

  public Double getMeanSquareError() {
    return getSampleStdDev() / Math.sqrt(numericValues);
  }

  /**
   * Returns the population skewness.
   * 
   * @return the population skewness.
   */
  public Double getPopulationSkewness() {
    if (unormalizedVariance == 0.0) {
      return 0.0;
    }
    return skew * Math.sqrt(numericValues) / Math.pow(unormalizedVariance, 3.0 / 2.0);
  }

  /**
   * Returns the sample skewness.
   * 
   * @return the sample skewness.
   */
  public Double getSampleSkewness() {
    if (numericValues <= 1.0) {
      return 0.0;
    }
    return getPopulationSkewness() * numericValues / (numericValues - 1);
  }

  public Double getSampleSkewnessStdError() {
    return Math.sqrt((6.0 * numericValues * (numericValues - 1.0))
        / ((numericValues - 2.0) * (numericValues + 1.0) * (numericValues + 3.0)));
  }

  /**
   * Returns the population kurtosis.
   * 
   * @return the population kurtosis.
   */
  public Double getPopulationKurtosis() {
    if (unormalizedVariance == 0.0) {
      return 0.0;
    }
    return (kurtosis * numericValues) / Math.pow(unormalizedVariance, 2.0);
  }

  /**
   * Returns the sample kurtosis.
   * 
   * @return the sample kurtosis.
   */
  public Double getSampleKurtosis() {
    if (numericValues <= 1.0) {
      return 0.0;
    }
    return getPopulationKurtosis() * numericValues / (numericValues - 1);
  }

  public Double getSampleKurtosisStdError() {
    return 2.0 * getSampleSkewnessStdError() * Math.sqrt(
        (Math.pow(numericValues, 2.0) - 1.0) / ((numericValues - 3.0) * (numericValues + 5.0)));
  }

  public Double getPopulationExcessOfKurtosis() {
    return getPopulationKurtosis() - 3.0;
  }

  public Double getSampleExcessOfKurtosis() {
    return getSampleKurtosis() - 3.0;
  }

  /**
   * Returns the Jarque Bera score.
   * 
   * @return the Jarque Bera score
   */
  public Double getSampleJarqueBeraScore() {
    if (numericValues == 0) {
      return Double.NaN;
    }
    return (numericValues / 6.0)
        * (Math.pow(getSampleSkewness(), 2.0) + 0.25 * Math.pow(getSampleExcessOfKurtosis(), 2.0));

  }

  /**
   * Returns the average length.
   * 
   * @return the average length
   */
  public Double getAvgLength() {
    return avgLength;
  }

  private void setAvgLength(Double avgLength) {
    this.avgLength = avgLength;
  }

  private long getNonNullValues() {
    return rowCount - nullValues;
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
   * @param datePattern the date pattern (if null it will use the default set of pattern)
   * @return the corresponding date object
   * @throws ParseException if the date string is invalid with respect to the passed pattern
   */
  public static java.sql.Date parseDate(Locale loc, String str, String datePattern)
      throws ParseException {
    if (datePattern == null) {
      return new java.sql.Date(
          DateUtils.parseDateStrictly(str, loc, DEFAULT_DATE_PATTERN_IN).getTime());
    } else {
      return new java.sql.Date(DateUtils.parseDateStrictly(str, loc, datePattern).getTime());
    }
  }

  // from calcite SqlFunctions.java
  private Integer getValLength(Object val) {
    if (val == null || val instanceof java.sql.Date) {
      return null;
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
    throw new IllegalArgumentException(
        "Class " + val.getClass().getCanonicalName() + " not handled yet");
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
    ret.append("\n Statistics of column[").append(columnIndex).append("]: '").append(columnName)
        .append("'");
    ret.append("\n----------------------------------------");
    ret.append("\nRow count: " + getRowCount());
    ret.append("\nNumeric values: " + getNullValues());
    ret.append("\nNull values: " + getNullValues());
    ret.append("\nEmpty strings: " + getEmptyString());
    ret.append("\nMinLength: " + getMinLength());
    ret.append("\nMaxLength: " + getMaxLength());
    ret.append(String.format("%nAvgLength: %.3f", getAvgLength()));

    // numeric values only
    if (numericValues > 0) {
      ret.append("\nMin: " + getMin());
      ret.append("\nMax: " + getMax());
      ret.append(String.format("%nMean: %.3f", getAvg()));
      ret.append(String.format("%nMean square error (MSE): %.3f", getMeanSquareError()));
      ret.append(String.format("%nVariance (population): %.3f", getPopulationVariance()));
      ret.append(String.format("%nStdDev (population): %.3f", getPopulationStdDev()));
      ret.append(String.format("%nVariance (sample): %.3f", getSampleVariance()));
      ret.append(String.format("%nVariance error (sample): %.3f", getSampleVarianceStdError()));
      ret.append(String.format("%nStdDev (sample): %.3f", getSampleStdDev()));
      ret.append(String.format("%nStdDev error (sample) : %.3f", getSampleStdDevStdError()));
      ret.append(String.format("%nSkewness (population): %.3f", getPopulationSkewness()));
      ret.append(String.format("%nSkewness (sample): %.3f", getSampleSkewness()));
      ret.append(String.format("%nSkewness error (sample): %.3f", getSampleSkewnessStdError()));
      ret.append(String.format("%nKurtosis (population): %.3f", getPopulationKurtosis()));
      ret.append(String.format("%nKurtosis (sample): %.3f", getSampleKurtosis()));
      ret.append(String.format("%nKurtosis error (sample): %.3f", getSampleKurtosisStdError()));

      printExcessOfKurtosis(ret, "population", getPopulationExcessOfKurtosis());
      printExcessOfKurtosis(ret, "sample", getSampleExcessOfKurtosis());

      final Double jarqueBeraScore = getSampleJarqueBeraScore();
      ret.append(String.format("%nJarqueBeraScore (sample ex): %.3f", jarqueBeraScore));
      printJarqueBeraInfo(ret, jarqueBeraScore);
    }
    // string values only
    if (topValues != null) {
      ret.append("\n-- TYPE STATS:");
      ret.append("\n\tboolean values: " + booleanValues + "");
      ret.append("\n\tint values: " + intValues);
      ret.append("\n\tlong values: " + longValues);
      ret.append("\n\tfloat values: " + floatValues);
      ret.append("\n\tdouble values: " + doubleValues);
      ret.append("\n\tdate values: " + dateValues);
      ret.append("\n-- TOP VALUES:");
      topValues.entrySet()//
          .stream()//
          .sorted((x, y) -> y.getValue().compareTo(x.getValue()))//
          .forEach(x -> ret.append("\n\t").append(x.getKey() + " => " + x.getValue()));
      ret.append("\n-- TOP PATTERNS:");
      topPatterns.entrySet()//
          .stream()//
          .sorted((x, y) -> y.getValue().compareTo(x.getValue()))//
          .forEach(x -> ret.append("\n\t").append(x.getKey() + " => " + x.getValue()));
    }
    return ret.toString();

  }

  private void printExcessOfKurtosis(StringBuilder ret, String type, final Double excess) {
    final String interpr = getKurtosisInterpretation(excess);
    ret.append(String.format("%nExcess of Kurtosis (%s): %.3f => %s", type, excess, interpr));
  }

  private String getKurtosisInterpretation(Double excess) {
    if (excess == 0) {
      // e.g. Normal distributions, Binomial
      return "Mesokurtic distibution";
    }
    if (excess > 0) { // distribution has "fatter tails
      // e.g. Student's t, Rayleigh, Laplace, Exponential, Poisson, Logistic
      return "Leptokurtik distibution (or super-Gaussian distributions)";
      // ..... REMARK: usually there are outliers so check the data...
    } // else excess < 0 distribution has "thinner tails"
    // e.g. Bernoulli
    return "Platykurtic distibution (or sub-Gaussian distributions)";
  }

  // jarqueBeraScore thresholds follows a chi-square distribution with df=2
  // (but only for high-cardinality samples)
  // http://academicos.fciencias.unam.mx/wp-content/uploads/sites/91/2015/04/jarque_bera_87.pdf
  // https://people.smp.uq.edu.au/YoniNazarathy/stat_models_B_course_spring_07/distributions/chisqtab.pdf
  private void printJarqueBeraInfo(StringBuilder ret, final Double jarqueBeraScore) {
    ret.append("\n\tHypothesis testing (Null hypothesis H0 = Field is normally distributed):");
    ret.append("\n\tWARNING: small sample size can leads to non-accurate conclusions (current size="
        + getNonNullValues() + ")");
    ret.append("\n\t\t----------------------------------------------");
    ret.append("\n\t\tAlpha    CDF   Critical Value     Conclusion");
    ret.append("\n\t\t---------------------------------------------");
    ret.append("\n\t\t 10%\t90.0%\t" + getJarqueBeraConclusion(jarqueBeraScore, 4.61));// .1
    ret.append("\n\t\t  5%\t95.0%\t" + getJarqueBeraConclusion(jarqueBeraScore, 5.99));// .05
    ret.append("\n\t\t2.5%\t97.5%\t" + getJarqueBeraConclusion(jarqueBeraScore, 7.38));// .025
    ret.append("\n\t\t  1%\t99.0%\t" + getJarqueBeraConclusion(jarqueBeraScore, 9.21));// .01
  }

  private String getJarqueBeraConclusion(final Double jarqueBeraScore, final double threshold) {
    return String.format("    %.3f\t  H0 %s", threshold,
        jarqueBeraScore > threshold ? "REJECTED" : "ACCEPTED");
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

  public Integer getMinLength() {
    return minLength;
  }

  public StatsPojo setMinLength(Integer minLength) {
    this.minLength = minLength;
    return this;
  }

  public Integer getMaxLength() {
    return maxLength;
  }

  public StatsPojo setMaxLength(Integer maxLength) {
    this.maxLength = maxLength;
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

  public Map<String, Integer> getTopValues() {
    return topValues;
  }

  public void setTopValues(Map<String, Integer> topValues) {
    this.topValues = topValues;
  }

  /**
   * Set top values utility method.
   * 
   * @param it the tuples iterator
   * @return this object
   */
  public StatsPojo setTopValues(Iterable<Tuple2<String, Long>> it) {
    this.topValues = new HashMap<>();
    if (it != null) {
      for (Tuple2<String, Long> tuple : it) {
        this.topValues.put(tuple.f0, tuple.f1.intValue());
      }
    }
    return this;
  }

  public Map<String, Integer> getTopPatterns() {
    return topPatterns;
  }

  public void setTopPatterns(Map<String, Integer> topPatterns) {
    this.topPatterns = topPatterns;
  }

  /**
   * Set top patterns utility method.
   * 
   * @param it the tuples iterator
   * @return this object
   */
  public StatsPojo setTopPatterns(Iterable<Tuple2<String, Long>> it) {
    this.topPatterns = new HashMap<>();
    if (it != null) {
      for (Tuple2<String, Long> tuple : it) {
        this.topPatterns.put(tuple.f0, tuple.f1.intValue());
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

  @Override
  public int compareTo(StatsPojo o) {
    return getColumnIndex() - o.getColumnIndex();
  }

}
