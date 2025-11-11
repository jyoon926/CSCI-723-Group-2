package edu.rit.gdb.a5;

import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import org.json.JSONArray;
import org.json.JSONObject;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class TransEEpoch {

	public TransEEpoch() {
		super();
	}

	private List<BigDecimal> toBigDecimalList(String[] strList) {
		return new ArrayList<>(Arrays.stream(strList).map(BigDecimal::new).toList());
	}

	private String[] toStringArray(List<BigDecimal> embedding) {
		return embedding.stream().map(BigDecimal::toPlainString).toArray(String[]::new);
	}

	private BigDecimal distance(List<BigDecimal> sEmb, List<BigDecimal> pEmb, List<BigDecimal> oEmb, String distanceStr) {
		BigDecimal d = BigDecimal.ZERO;
		for (int i = 0; i < sEmb.size(); i++) {
			BigDecimal diff = sEmb.get(i).add(pEmb.get(i)).subtract(oEmb.get(i));
			if (distanceStr.equals("L1"))
				d = d.add(diff.abs());
			else
				d = d.add(diff.multiply(diff, MathContext.DECIMAL128));
		}
		if (distanceStr.equals("L2"))
			d = d.sqrt(MathContext.DECIMAL128);
		return d;
	}

	@Procedure(name = "gdb.gradientDescent", mode = Mode.WRITE)
	public void gradientDescent(@Name("s") Node s, @Name("p") Relationship p, @Name("o") Node o, @Name("sp") Node sp,
			@Name("op") Node op, @Name("gamma") String gammaStr, @Name("distance") String distanceStr,
			@Name("alpha") String alphaStr) {
		List<BigDecimal> sEmb = toBigDecimalList((String[]) s.getProperty("embedding"));
		List<BigDecimal> pEmb = toBigDecimalList((String[]) p.getProperty("embedding"));
		List<BigDecimal> oEmb = toBigDecimalList((String[]) o.getProperty("embedding"));
		List<BigDecimal> spEmb = sp.equals(s) ? sEmb : toBigDecimalList((String[]) sp.getProperty("embedding"));
		List<BigDecimal> opEmb = op.equals(o) ? oEmb : toBigDecimalList((String[]) op.getProperty("embedding"));
		BigDecimal alpha = new BigDecimal(alphaStr);
		BigDecimal gamma = new BigDecimal(gammaStr);

		// Compute distance for positive and negative triples
		BigDecimal dPos = distance(sEmb, pEmb, oEmb, distanceStr);
		BigDecimal dNeg = distance(spEmb, pEmb, opEmb, distanceStr);

		if (gamma.add(dPos).subtract(dNeg).compareTo(BigDecimal.ZERO) <= 0) {
			return; // No update needed
		}

		// Update embeddings
		for (int i = 0; i < sEmb.size(); i++) {
			BigDecimal xi = BigDecimal.TWO.multiply(sEmb.get(i).add(pEmb.get(i)).subtract(oEmb.get(i)));
			BigDecimal xpi = BigDecimal.TWO.multiply(spEmb.get(i).add(pEmb.get(i)).subtract(opEmb.get(i)));

			if (distanceStr.equals("L1")) {
				xi = BigDecimal.valueOf(xi.signum());
				xpi = BigDecimal.valueOf(xpi.signum());
			}

			sEmb.set(i, sEmb.get(i).subtract(alpha.multiply(xi)));
			oEmb.set(i, oEmb.get(i).add(alpha.multiply(xi)));
			spEmb.set(i, spEmb.get(i).add(alpha.multiply(xpi)));
			opEmb.set(i, opEmb.get(i).subtract(alpha.multiply(xpi)));
			pEmb.set(i, pEmb.get(i).subtract(alpha.multiply(xi)).add(alpha.multiply(xpi)));
		}

		s.setProperty("embedding", toStringArray(sEmb));
		o.setProperty("embedding", toStringArray(oEmb));
		if (!sp.equals(s)) sp.setProperty("embedding", toStringArray(spEmb));
		if (!op.equals(o)) op.setProperty("embedding", toStringArray(opEmb));
		p.setProperty("embedding", toStringArray(pEmb));
	}

	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0], jsonFile = args[1];

		String[] jsonLines = Files.readString(Path.of(jsonFile)).split("\n");
		for (String line : jsonLines) {
			JSONObject json = new JSONObject(line);
			String kg = json.getString("kg");
			String alpha = String.valueOf(json.getDouble("alpha"));
			String gamma = String.valueOf(json.getDouble("gamma"));
			String distance = json.getString("dist");
			JSONArray batch = json.getJSONArray("batch");

			try (DatabaseManagementService serviceDb = getNeo4jConnection(neo4jFolder, kg);) {
				GraphDatabaseService db = serviceDb.database(GraphDatabaseSettings.initial_default_database.defaultValue());
				DependencyResolver resolver = ((GraphDatabaseAPI) db).getDependencyResolver();
				GlobalProcedures procedures = resolver.resolveDependency(GlobalProcedures.class, DependencyResolver.SelectionStrategy.SINGLE);
				procedures.registerProcedure(TransEEpoch.class);

				// TODO Use the batch provided as input as well as the other hyperparemeters
				// (alpha, gamma and dist) to perform gradient descent. Recall that the
				// embeddings must be updated only if gamma + d(s, p, o) - d(s', p, o') is less
				// than zero.

				db.executeTransactionally("CREATE INDEX entity_index IF NOT EXISTS FOR (e:Entity) ON e.id");

				// Get elementIds of predicates with embeddings
				Map<String, String> predicateIds = db.executeTransactionally(
						"""
							MATCH ()-[p]->()
							WITH type(p) AS label, p
							ORDER BY p.id
							WITH label, collect(p)[0] AS minP
							RETURN collect([label, elementId(minP)]) AS pairs
						""",
						Map.of(),
						r -> ((List<List<String>>) r.next().get("pairs")).stream().collect(Collectors.toMap(l -> l.get(0), l -> l.get(1)))
				);

				long startTime = System.nanoTime();
				for (int i = 0; i < batch.length(); i++) {
					JSONObject sample = batch.getJSONObject(i);
					long s = sample.getInt("s");
					long o = sample.getInt("o");
					long sp = sample.getInt("sp");
					long op = sample.getInt("op");
					String p = sample.getString("p");
					String pId = predicateIds.get(p);
					db.executeTransactionally(
							"""
								MATCH ()-[p]->()
								WHERE elementId(p) = $p
								MATCH (s:Entity {id: $s})
								MATCH (o:Entity {id: $o})
								MATCH (sp:Entity {id: $sp})
								MATCH (op:Entity {id: $op})
								CALL gdb.gradientDescent(s, p, o, sp, op, $gamma, $dist, $alpha)
							""",
							Map.of("p", pId, "s", s, "o", o, "sp", sp, "op", op, "gamma", gamma, "dist", distance, "alpha", alpha)
					);
				}
				long elapsedTimeNanos = System.nanoTime() - startTime;
				double elapsedTimeSeconds = (double) elapsedTimeNanos / 1_000_000_000;
				System.out.println("- Updating embeddings took: " + elapsedTimeSeconds + "s");
			}
		}
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
