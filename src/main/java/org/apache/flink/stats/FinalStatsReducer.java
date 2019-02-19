package org.apache.flink.stats;

import org.apache.flink.api.common.functions.ReduceFunction;

public class FinalStatsReducer implements ReduceFunction<StatsPojo> {
	private static final long serialVersionUID = 1L;

	@Override
	public StatsPojo reduce(StatsPojo s1, StatsPojo s2) throws Exception {
		if (StatsPojo.TYPE_BASIC.equals(s2.getStatsType())) {
			if (s1.getTopValues() != null) {
				s2.setTopValues(s1.getTopValues());
			}
			if (s1.getTopPatterns() != null) {
				s2.setTopPatterns(s1.getTopPatterns());
			}
			return s2;
		}
		// here both s1 and s2 are not the base stats
		if (s2.getTopValues() != null) {
			s1.setTopValues(s2.getTopValues());
		}
		if (s2.getTopPatterns() != null) {
			s1.setTopPatterns(s2.getTopPatterns());
		}
		return s1;
	}
}