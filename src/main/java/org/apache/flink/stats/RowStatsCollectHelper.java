package org.apache.flink.stats;

import java.io.IOException;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

/**
 * Inspired by Utils.CollectHelper
 */
public class RowStatsCollectHelper extends RichOutputFormat<Row> {

  private static final long serialVersionUID = 1L;

  public static final String COMPLETE_RECORDS_ACC_ID = "complete-records-counter";
  private LongCounter completeRecordsAcc;

  @Override
  public void configure(Configuration parameters) {
    // do nothing
  }

  @Override
  public void open(int taskNumber, int numTasks) {
    this.completeRecordsAcc = new LongCounter();
  }

  @Override
  public void writeRecord(Row row) throws IOException {
    for (int i = 0; i < row.getArity(); i++) {
      if (row.getField(i) == null || row.getField(i).toString().trim().isEmpty()) {
        return;
      }
    }
    completeRecordsAcc.add(1L);
  }

  @Override
  public void close() {
    // Important: should only be added in close method to minimize traffic of
    // accumulators
    getRuntimeContext().addAccumulator(COMPLETE_RECORDS_ACC_ID, completeRecordsAcc);
  }
}