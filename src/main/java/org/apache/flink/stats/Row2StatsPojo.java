package org.apache.flink.stats;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

public class Row2StatsPojo implements MapFunction<Row, StatsPojo> {

	private static final long serialVersionUID = 1L;
	private int colIndex;

	public Row2StatsPojo(int colIndex) {
		this.colIndex = colIndex;
	}

	@Override
	public StatsPojo map(Row cell) throws Exception {
		final StatsPojo ret = new StatsPojo(colIndex);
		final Double doubleVal = ret.updateStats(cell.getField(0));
		if (doubleVal != null) {
			ret.setSum(doubleVal);
		}
		return ret;
	}
}