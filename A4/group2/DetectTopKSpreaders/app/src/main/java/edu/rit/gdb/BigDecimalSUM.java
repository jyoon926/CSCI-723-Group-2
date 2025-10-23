package edu.rit.gdb;

import java.math.BigDecimal;

import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;

public class BigDecimalSUM {
	public BigDecimalSUM() {
		super();
	}

	@UserAggregationFunction(value = "gdb.BDSUM")
	public BigDecimalAggregator BDSUM() {
		return new BigDecimalAggregator();
	}

	public static class BigDecimalAggregator {
		private BigDecimal accumulated = BigDecimal.ZERO;

		@UserAggregationUpdate
		public void add(@Name("string") String other) {
			accumulated = accumulated.add(new BigDecimal(other));
		}

		@UserAggregationResult
		public String result() {
			return accumulated.toString();
		}
	}
}