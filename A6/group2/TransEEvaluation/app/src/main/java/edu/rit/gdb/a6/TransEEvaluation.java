package edu.rit.gdb.a6;

import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONObject;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

public class TransEEvaluation {

	public TransEEvaluation() {
		super();
	}

	@UserFunction(name = "gdb.distance")
	public String distance(@Name("s") Node s, @Name("p") Relationship p, @Name("o") Node o,
			@Name("distance") String distanceStr) {
		MathContext mc = MathContext.DECIMAL128;
		BigDecimal total = BigDecimal.ZERO;
		String[] sEmb = (String[]) s.getProperty("embedding");
		String[] pEmb = (String[]) p.getProperty("embedding");
		String[] oEmb = (String[]) o.getProperty("embedding");

		if (sEmb == null || pEmb == null || oEmb == null){
			return BigDecimal.ZERO.toString();
		}

		for (int i = 0; i < sEmb.length; i++) {
			BigDecimal t = new BigDecimal(sEmb[i]).add(new BigDecimal(pEmb[i]), mc).subtract(new BigDecimal(oEmb[i]), mc).abs();
			if (distanceStr.equals("L2")) {
				t = t.pow(2, mc);
			}
			total = total.add(t, mc);
		}
		if (distanceStr.equals("L2")){
			total = total.sqrt(mc);
		}
		return total.toString();
	}

	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0], jsonFile = args[1];
		String[] jsonLines = Files.readString(Path.of(jsonFile)).split("\n");
		//for (String line : jsonLines) {
		String line = jsonLines[0];
			JSONObject json = new JSONObject(line);
			String kg = json.getString("kg");
			String distance = json.getString("dist"); // Either L1 or L2

			try (DatabaseManagementService serviceDb = getNeo4jConnection(neo4jFolder, kg);) {
				GraphDatabaseService db = serviceDb
						.database(GraphDatabaseSettings.initial_default_database.defaultValue());

				DependencyResolver resolver = ((GraphDatabaseAPI) db).getDependencyResolver();
				GlobalProcedures procedures = resolver.resolveDependency(GlobalProcedures.class,
						DependencyResolver.SelectionStrategy.SINGLE);

				procedures.registerFunction(TransEEvaluation.class);

				// For each fact in each split, you must compute eight numbers. They are
				// the rank of the fact and total number of negatives according to different
				// corruptions and negative identification.
				// - Corrupting the subject using sensical negatives: ranks_sensical, r.totals_sensical
				// - Corrupting the object using sensical negatives: ranko_sensical, r.totalo_sensical
				// - Corrupting the subject using nonsensical negatives: ranks_nonsensical, r.totals_nonsensical
				// - Corrupting the object using nonsensical negatives: ranko_nonsensical, r.totalo_nonsensical

				for (Long split : new Long[]{Long.valueOf(1), Long.valueOf(2)}) {
				// if split = 1 (valid), Gx = valid, Gy = training u valid (0 u 1)
				// if split = 2 (test), Gx = test, Gy = training u valid u test

					// For now: Sensical, Valid
					//Long split = Long.valueOf(1);
					List<List<Long>> gx = db.executeTransactionally(
							"MATCH (s)-[p {split: $split}]->(o) return collect([s.id, p.id, o.id]) as t",
							Map.of("split", split),
							res -> res.hasNext() ? ((List<List<Long>>) res.next().get("t")) : List.of());

					// 	Retrieved: {
					// 	properties(o)={orig_embeddding=[Ljava.lang.String;@5c85ed05, description=uk, id=1, embedding=[Ljava.lang.String;@23657729},
					// 	properties(p)={orig_embedding=[Ljava.lang.String;@365291bd, split=0, id=0, embedding=[Ljava.lang.String;@68c05c5b}}

					// for each p(s, o) in Gx (facts to evaluate)
					for (List<Long> t : gx) {
						long s = t.get(0);
						long p = t.get(1);
						long o = t.get(2);

						System.out.println("Triple: (" + s + ", " + p + ", " + o + ")");

						// get embedding of p
						long pid = (long) db.executeTransactionally(
								"MATCH  ()-[p {id: $curr_pid}]->(), ()-[p0]->() " +
										"WHERE p0.embedding is not null AND type(p) = type(p0) RETURN p0.id as pid",
								Map.of("curr_pid", p),
								res -> res.next().get("pid"));

						// y = f(s, p, o) --> score
						String score = (String) db.executeTransactionally(
								"MATCH (s {id: $subject}) " +
										"MATCH ()-[p {id: $pid}]->() " +
										"MATCH (o {id: $object}) " +
										"RETURN gdb.distance(s, p, o, $dist) as distance",
								Map.of("subject", s, "pid", pid, "object", o, "dist", distance),
								res -> res.next().get("distance"));
						BigDecimal y = new BigDecimal(score, MathContext.DECIMAL128);

						// sensical vs nonsensical
						for (boolean isSensical : new boolean[]{true, false}) {
							int totals_negs = 0;
							int rs_greater = 1;
							int rs_equal = 0;
							// for each s' in Entities(range/domain of Gy) and p(s', o) not in Gy (if s' is a negative)
							Set<Long> domain = getDomainOrRange(db, pid, isSensical, split);
							for (Long s_prime : domain) {
								Long pid_prime = db.executeTransactionally("MATCH (sp {id: $sid})-[p]->(o {id: $oid}), " +
												"()-[p0 {id: $pid}]-() WHERE p.split <= $split AND type(p) = type(p0) RETURN p.id",
										Map.of("sid", s_prime, "pid", pid, "oid", o, "split", split),
										res -> res.hasNext() ? ((Long) res.next().get("p.id")) : -1);
								// if -1, then result was not found and it is a negative
								if (pid_prime == -1) {
									String score_prime = (String) db.executeTransactionally(
											"MATCH (s {id: $subject}) " +
													"MATCH ()-[p {id: $pid}]->() " +
													"MATCH (o {id: $object}) " +
													"RETURN gdb.distance(s, p, o, $dist) as distance",
											Map.of("subject", s_prime, "pid", pid, "object", o, "dist", distance),
											res -> res.next().get("distance"));
									BigDecimal y_prime = new BigDecimal(score_prime, MathContext.DECIMAL128);  // y' = f(s', p, o)
									if (y_prime.compareTo(y) > 0) {
										rs_greater++;
										//System.out.println("Negative (" + s_prime + ", p, " + o + ") scored higher.");
									}
									if (y_prime.compareTo(y) == 0) {
										rs_equal++;
										//System.out.println("Negative (" + s_prime + ", p, " + o + ") scored equal.");
									}
									if (y_prime.compareTo(y) < 0) {
										//System.out.println("Negative (" + s_prime + ", p, " + o + ") score lower.");
									}
									totals_negs++;
								}
							}
							double rs = ((2.0 * rs_greater) + rs_equal) / 2.0;
							System.out.println("rs: " + rs);
							System.out.println("num negs: " + totals_negs);
							if (isSensical){
								db.executeTransactionally("MATCH ()-[p {id: $pid}]->() SET p.ranks_sensical = $rs, p.totals_sensical = $t",
										Map.of("pid", p, "rs", rs, "t", totals_negs));
							} else {
								db.executeTransactionally("MATCH ()-[p {id: $pid}]->() SET p.ranks_nonsensical = $rs, p.totals_nonsensical = $t",
										Map.of("pid", p, "rs", rs, "t", totals_negs));
							}

							// repeat for o (i know, not good code reuse)
							// for each o' in Entities(range/domain of Gy) and p(s, o') not in Gy (if o' is a negative)
							int totalo_negs = 0;
							int ro_greater = 1;
							int ro_equal = 0;
							Set<Long> range = getDomainOrRange(db, pid, !isSensical, split);
							for (Long o_prime : range) {
								Long pid_prime = db.executeTransactionally("MATCH (sp {id: $sid})-[p]->(o {id: $oid}), " +
												"()-[p0 {id: $pid}]-() WHERE p.split <= $split AND type(p) = type(p0) RETURN p.id",
										Map.of("sid", s, "pid", pid, "oid", o_prime, "split", split),
										res -> res.hasNext() ? ((Long) res.next().get("p.id")) : -1);
								// if -1, then result was not found and it is a negative
								if (pid_prime == -1) {
									String score_prime = (String) db.executeTransactionally(
											"MATCH (s {id: $subject}) " +
													"MATCH ()-[p {id: $pid}]->() " +
													"MATCH (o {id: $object}) " +
													"RETURN gdb.distance(s, p, o, $dist) as distance",
											Map.of("subject", s, "pid", pid, "object", o_prime, "dist", distance),
											res -> res.next().get("distance"));
									BigDecimal y_prime = new BigDecimal(score_prime, MathContext.DECIMAL128);  // y' = f(s', p, o)
									if (y_prime.compareTo(y) > 0) {
										ro_greater++;
									}
									if (y_prime.compareTo(y) == 0) {
										ro_equal++;
									}
									totalo_negs++;
								}
							}
							double ro = ((2.0 * ro_greater) + ro_equal) / 2.0;
							if (isSensical){
								db.executeTransactionally("MATCH ()-[p {id: $pid}]->() SET p.ranko_sensical = $ro, p.totalo_sensical = $t",
										Map.of("pid", p, "ro", ro, "t", totalo_negs));
							} else {
								db.executeTransactionally("MATCH ()-[p {id: $pid}]->() SET p.ranko_nonsensical = $ro, p.totalo_nonsensical = $t",
										Map.of("pid", p, "ro", ro, "t", totalo_negs));
							}
						}
					}
				}
			}
		//} //end for loop i think
	}

	private static Set<Long> getDomainOrRange(GraphDatabaseService db, long pid, boolean isDomain, long split) {
		String returnVar = isDomain ? "s" : "o"; // For domain, return s. For range, return o.
		String query = String.format(
				"MATCH (s)-[r]->(o), ()-[p {id: $pid}]->() WHERE r.split <= $split AND type(r) = type(p) WITH DISTINCT %s RETURN COLLECT(id(%s)) AS ids",
				returnVar, returnVar
		);
		return db.executeTransactionally(query, Map.of("pid", pid, "split", split), r -> new HashSet<>(((List<Long>) r.next().get("ids"))));
	}

	private static DatabaseManagementService getNeo4jConnection(String neo4jFolder, String database) {
		DatabaseManagementServiceBuilder builder = new DatabaseManagementServiceBuilder(Path.of(neo4jFolder, database))
				// This is necessary when dealing with large transactions... does it work?
				.setConfig(GraphDatabaseSettings.keep_logical_logs, "false")
				.setConfig(GraphDatabaseSettings.preallocate_logical_logs, false)
				.setConfig(GraphDatabaseSettings.memory_transaction_database_max_size, 0l)
				// This cleans the transaction files every 5 secs.
				.setConfig(GraphDatabaseSettings.check_point_interval_time, Duration.ofSeconds(5l));
		DatabaseManagementService service = builder.build();
		registerShutdownHook(service);
		return service;
	}

	private static void registerShutdownHook(final DatabaseManagementService service) {
		// Registers a shutdown hook for the Neo4j instance so that it
		// shuts down nicely when the VM exits (even if you "Ctrl-C" the
		// running application).
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				service.shutdown();
			}
		});
	}
}
