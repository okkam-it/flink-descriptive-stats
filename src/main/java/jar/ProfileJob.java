package jar;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.SerializedListAccumulator;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.stats.FinalStatsReducer;
import org.apache.flink.stats.RowStatsCollectHelper;
import org.apache.flink.stats.StatsPojo;
import org.apache.flink.stats.StringStatsTuple;
import org.apache.flink.types.Row;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Collector;

//Mainly inspired by:
//- (Philippe Pebay) http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.214.8508&rep=rep1&type=pdf
// -(Chan, Golub, LeVeque): http://i.stanford.edu/pub/cstr/reports/cs/tr/79/773/CS-TR-79-773.pdf
// -(Philippe Pebay) https://prod-ng.sandia.gov/techlib-noauth/access-control.cgi/2008/086212.pdf
//Standard errors: 
// - (Ahn, Fessler) http://web.eecs.umich.edu/~fessler/papers/files/tr/stderr.pdf
//Skew and kurtosis: 
// - (Stuart McCrary) https://www.thinkbrg.com/media/publication/720_720_McCrary_ImplementingAlgorithms_Whitepaper_20151119_WEB.pdf
// Jarque Bera score:
// - http://academicos.fciencias.unam.mx/wp-content/uploads/sites/91/2015/04/jarque_bera_87.pdf
public class ProfileJob {

  private static final int TOP_SIZE = 20;
  private static final String[] COLUMN_NAMES = new String[] { "col1", "col2", "col3" };
  private static final int NUM_ELEMENTS = 100_000;

  private static Row[] getRowArray(int size) {
    // java.util.Random rand = new java.util.Random(0);
    // Row[] ret = new Row[size];
    // for (int i = 0; i < size; i++) {
    // ret[i] = Row.of(true, "3", rand.nextGaussian() * 100);
    // }

    Row[] ret = new Row[size];
    for (int i = 0; i < size; i++) {
      if (i % 3 == 0) {
        // ret[i] = Row.of(true, "" + i, 5000);
        ret[i] = Row.of(true, "3", 1);
      } else if (i % 7 == 0) {
        ret[i] = Row.of(true, "7", 1);
      } else if (i % 11 == 0) {
        ret[i] = Row.of(null, "ABCDEF00X30A333Y", i);
      } else {
        ret[i] = Row.of(true, "" + i, i);
      }
    }
    return ret;
  }

  // TODO compute distinct rows
  // TODO compute quartiles
  // TODO compute histograms (requires bin size estimation)
  // TODO compute outliers count
  // TODO compute correlation

  /**
   * Main test.
   * 
   * @param args the arguments
   */
  public static void main(String[] args) throws Exception {

    final ParameterTool params = ParameterTool.fromArgs(args);
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // make parameters available in the web interface
    env.getConfig().setGlobalJobParameters(params);

    final DataSet<Row> rows = env.fromElements(getRowArray(NUM_ELEMENTS));
    final RowTypeInfo rowsType = (RowTypeInfo) rows.getType();

    // compute row-level stats
    rows.output(new RowStatsCollectHelper());

    // compute colum-level stats
    DataSet<StatsPojo> columnStats = null;
    for (int i = 0; i < COLUMN_NAMES.length; i++) {
      final int colIndex = i;
      final DataSet<Row> columnData = rows//
          .map(row -> Row.project(row, new int[] { colIndex })).name("project(" + colIndex + ")")//
          .returns(new RowTypeInfo(rowsType.getFieldTypes()[colIndex]));

      // basic column stats
      final DataSet<StatsPojo> basicStats = columnData //
          .map(r -> new StatsPojo(r.getField(0), colIndex)).name("row => statsPojo")//
          .reduce((v1, v2) -> v1.reduce(v2)).name("reduceStatsPojos()");

      // merge all (basic) column stats
      columnStats = columnStats == null ? basicStats
          : columnStats.union(basicStats).name("addBasicStatsOfCol(" + colIndex + ")");

      // add advances statistics => expensive TODO: make it configurable
      if (BasicTypeInfo.STRING_TYPE_INFO.equals(rowsType.getFieldTypes()[colIndex])) {
        columnStats = computeAdvancedStringStats(columnStats, colIndex, columnData);
      }
    }

    if (columnStats == null) {
      System.out.println("No stats computed. Check job params");
      System.exit(0);
    }
    final DataSet<StatsPojo> columnStatDs = columnStats//
        .groupBy(StatsPojo::getColumnIndex)// group by column index
        .reduce(new FinalStatsReducer());

    final String collectAccId = new AbstractID().toString();
    final TypeSerializer<StatsPojo> serializer = columnStatDs.getType()
        .createSerializer(env.getConfig());
    columnStatDs.output(new Utils.CollectHelper<>(collectAccId, serializer)).name("collectStats()");
    JobExecutionResult jobRes = env.execute();

    // print row-level stats
    final Long completeRecordsCount = jobRes
        .getAccumulatorResult(RowStatsCollectHelper.COMPLETE_RECORDS_ACC_ID);
    System.out.println("\n\n\nNumber of complete records: " + completeRecordsCount);

    // print column-level stats
    final List<StatsPojo> colStats = deserializeCollectedStatsPojo(collectAccId, serializer,
        jobRes);
    for (int i = 0; i < colStats.size(); i++) {
      final StatsPojo columnStat = colStats.get(i);
      System.out.println(columnStat.toString(COLUMN_NAMES[i], TOP_SIZE));
      estimateHistogramWidth(i, columnStat);
    }

  }

  // http://biocurious.com/files/histo-bin-size-scott.pdf
  private static void estimateHistogramWidth(int i, final StatsPojo columnStat) {
    double stdDevPop = columnStat.getPopulationStdDev();
    long rowSize = columnStat.getRowCount();
    double binWidth = Math.floor(3.49 * stdDevPop * Math.pow(rowSize, -1.0 / 3));
    double skewness = Math.ceil(columnStat.getPopulationSkewness());
    if (skewness == 1) {
      binWidth *= 0.4;
    } else if (skewness == 2) {
      binWidth *= 0.6;
    } else if (skewness == 3) {
      binWidth *= 0.7;
    } else if (skewness > 3) {
      System.out.println("Estimation of binWidth is not reliable..");
    }
    System.out
        .println("===================== BinWidth for column " + COLUMN_NAMES[i] + " = " + binWidth);
  }

  private static List<StatsPojo> deserializeCollectedStatsPojo(final String id,
      final TypeSerializer<StatsPojo> serializer, JobExecutionResult jobRes) {
    final List<StatsPojo> columnStatList;
    final ArrayList<byte[]> accResult = jobRes.getAccumulatorResult(id);
    if (accResult != null) {
      try {
        columnStatList = SerializedListAccumulator.deserializeList(accResult, serializer);
        Collections.sort(columnStatList);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Cannot find type class of collected data type.", e);
      } catch (IOException e) {
        throw new RuntimeException("Serialization error while deserializing collected data", e);
      }
    } else {
      throw new RuntimeException("The call to collect() could not retrieve the DataSet.");
    }
    return columnStatList;
  }

  private static DataSet<StatsPojo> computeAdvancedStringStats(DataSet<StatsPojo> columnStats,
      final int colIndex, final DataSet<Row> columnData) {
    final DataSet<StringStatsTuple> stringStats = columnData //
        .filter(row -> row.getField(0) != null) // remove null values
        .map(row -> new StringStatsTuple(colIndex, (String) row.getField(0))) //
        .name("statsPojo => stringStatsTuple");
    // TOP K values
    final DataSet<Tuple2<String, Long>> valuePairs = stringStats//
        .project(StringStatsTuple.STRING_VALUE_POS, StringStatsTuple.COUNTER_POS);
    final DataSet<Tuple2<String, Long>> topValues = valuePairs//
        .groupBy(0)//
        .sum(1)//
        .sortPartition(1, Order.DESCENDING).setParallelism(1)//
        .first(TOP_SIZE);
    final DataSet<StatsPojo> wordFreq = topValues//
        .groupBy(x -> 0)// fake group key
        .reduceGroup(new ReduceTopStrings(colIndex)).name("reduceTopStrings()");

    // TOP K PATTERNS
    final DataSet<Tuple2<String, Long>> patternPairs = stringStats//
        .project(StringStatsTuple.PATTERN_POS, StringStatsTuple.COUNTER_POS);
    final DataSet<Tuple2<String, Long>> topPatterns = patternPairs//
        .groupBy(0)//
        .sum(1)//
        .sortPartition(1, Order.DESCENDING).setParallelism(1)//
        .first(TOP_SIZE);
    final DataSet<StatsPojo> patternFreq = topPatterns//
        .groupBy(x -> 0)// fake group key
        .reduceGroup(new ReduceTopPatterns(colIndex))//
        .name("reduceTopPatterns()");

    // merge all stats
    return columnStats.union(wordFreq).union(patternFreq);
  }

  private static final class ReduceTopPatterns
      implements GroupReduceFunction<Tuple2<String, Long>, StatsPojo> {
    private final int colIndex;
    private static final long serialVersionUID = 1L;

    private ReduceTopPatterns(int colIndex) {
      this.colIndex = colIndex;
    }

    @Override
    public void reduce(Iterable<Tuple2<String, Long>> it, Collector<StatsPojo> out)
        throws Exception {
      out.collect(new StatsPojo(colIndex, StatsPojo.TYPE_PATTERN).setTopPatterns(it));
    }
  }

  private static final class ReduceTopStrings
      implements GroupReduceFunction<Tuple2<String, Long>, StatsPojo> {
    private final int colIndex;
    private static final long serialVersionUID = 1L;

    private ReduceTopStrings(int colIndex) {
      this.colIndex = colIndex;
    }

    @Override
    public void reduce(Iterable<Tuple2<String, Long>> it, Collector<StatsPojo> out)
        throws Exception {
      out.collect(new StatsPojo(colIndex, StatsPojo.TYPE_WORD).setTopValues(it));
    }
  }

}
