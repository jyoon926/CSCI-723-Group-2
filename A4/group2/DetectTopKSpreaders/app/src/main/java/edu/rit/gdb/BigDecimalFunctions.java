package edu.rit.gdb;

import java.math.BigDecimal;
import java.math.MathContext;

import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

public class BigDecimalFunctions {
	@UserFunction("gdb.abs")
	public String add(@Name("o") String o) {
		return new BigDecimal(o).abs().toString();
	}

	@UserFunction("gdb.add")
	public String add(@Name("o1") String o1, @Name("o2") String o2) {
		return new BigDecimal(o1).add(new BigDecimal(o2)).toString();
	}

	@UserFunction("gdb.subtract")
	public String subtract(@Name("o1") String o1, @Name("o2") String o2) {
		return new BigDecimal(o1).subtract(new BigDecimal(o2)).toString();
	}

	@UserFunction("gdb.multiply")
	public String multiply(@Name("o1") String o1, @Name("o2") String o2) {
		return new BigDecimal(o1).multiply(new BigDecimal(o2)).toString();
	}

	@UserFunction("gdb.divide")
	public String divide(@Name("o1") String o1, @Name("o2") String o2) {
		return new BigDecimal(o1).divide(new BigDecimal(o2), MathContext.DECIMAL128).toString();
	}

	@UserFunction("gdb.sqrt")
	public String sqrt(@Name("o") String o) {
		return new BigDecimal(o).sqrt(MathContext.DECIMAL128).toString();
	}
}
