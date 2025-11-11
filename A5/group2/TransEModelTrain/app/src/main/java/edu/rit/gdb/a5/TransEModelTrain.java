package edu.rit.gdb.a5;

import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import org.json.JSONObject;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class TransEModelTrain {
	public static Random random = new Random();
	public static MathContext MC = MathContext.DECIMAL128;

	public TransEModelTrain() {
		super();
	}

	private String[] generateEmbedding(long dimension) {
		String[] embedding = new String[(int) dimension];
		for (int i = 0; i < dimension; i++) {
			BigDecimal v = (new BigDecimal((random.nextLong(100) + 1))).divide(new BigDecimal(100), MC);
			BigDecimal max = BigDecimal.valueOf(6).divide(BigDecimal.valueOf(dimension).sqrt(MC), MC);
			v = v.multiply(max.multiply(BigDecimal.TWO, MC), MC).subtract(max);
			embedding[i] = v.toPlainString();
		}
		return embedding;
	}

	private String[] normalizeEmbedding(String[] embedding) {
		BigDecimal norm = new BigDecimal(0);
        for (String s : embedding) {
            BigDecimal v = new BigDecimal(s);
            norm = norm.add(v.multiply(v, MC));
        }
		norm = norm.sqrt(MC);
		for (int i = 0; i < embedding.length; i++) {
			BigDecimal v = new BigDecimal(embedding[i]);
			v = v.divide(norm, MC);
			embedding[i] = v.toPlainString();
		}
		return embedding;
	}

	private List<BigDecimal> toBigDecimalList(String[] strList) {
		return new ArrayList<>(Arrays.stream(strList).map(BigDecimal::new).toList());
	}

	private String[] toStringArray(List<BigDecimal> embedding) {
		return embedding.stream().map(BigDecimal::toPlainString).toArray(String[]::new);
	}

	@Procedure(name = "gdb.initializeEntity", mode = Mode.WRITE)
	public void initialize(@Name("x") Node x, @Name("dim") long dimension) {
		x.setProperty("embedding", generateEmbedding(dimension));
	}

	@Procedure(name = "gdb.initializePredicate", mode = Mode.WRITE)
	public void initialize(@Name("x") Relationship x, @Name("dim") long dimension) {
		x.setProperty("embedding", normalizeEmbedding(generateEmbedding(dimension)));
	}

	@Procedure(name = "gdb.normalizeEntity", mode = Mode.WRITE)
	public void normalize(@Name("x") Node x) {
		x.setProperty("embedding", normalizeEmbedding((String[]) x.getProperty("embedding")));
	}

	@Procedure(name = "gdb.normalizePredicate", mode = Mode.WRITE)
	public void normalize(@Name("x") Relationship x) {
		x.setProperty("embedding", normalizeEmbedding((String[]) x.getProperty("embedding")));
	}

	@Procedure(name = "gdb.scalePredicate", mode = Mode.WRITE)
	public void scale(@Name("x") Relationship x) {
		List<BigDecimal> embedding = toBigDecimalList((String[]) x.getProperty("embedding"));

		// Find min and max
		BigDecimal min = embedding.stream().min(BigDecimal::compareTo).orElseThrow();
		BigDecimal max = embedding.stream().max(BigDecimal::compareTo).orElseThrow();
		BigDecimal range = max.subtract(min);

		int dim = embedding.size();

		BigDecimal maxVal = BigDecimal.valueOf(6).divide(BigDecimal.valueOf(dim).sqrt(MC), MC);

		// Min-max scale to [0,1], then to [-maxVal, maxVal]
		for (int i = 0; i < dim; i++) {
			BigDecimal scaled = embedding.get(i).subtract(min).divide(range, MC);
			scaled = scaled.multiply(maxVal.multiply(BigDecimal.TWO)).subtract(maxVal);
			embedding.set(i, scaled);
		}

		x.setProperty("embedding", toStringArray(embedding));
	}

	private BigDecimal distance(List<BigDecimal> sEmb, List<BigDecimal> pEmb, List<BigDecimal> oEmb, String distanceStr) {
		BigDecimal d = BigDecimal.ZERO;
		for (int i = 0; i < sEmb.size(); i++) {
			BigDecimal diff = sEmb.get(i).add(pEmb.get(i)).subtract(oEmb.get(i));
			if (distanceStr.equals("L1"))
				d = d.add(diff.abs());
			else
				d = d.add(diff.multiply(diff, MC));
		}
		if (distanceStr.equals("L2"))
			d = d.sqrt(MC);
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

	private static void Train(
			GraphDatabaseService db,
			int dim,
			int batchSize,
			int negativeRate,
			int epochs,
			String alpha,
			String gamma,
			String distance) {
		Transaction tx = db.beginTx();

		// Initialize entity embeddings
		db.executeTransactionally(
				"""
					MATCH (e:Entity)
					WITH e ORDER BY e.id
					CALL {WITH e CALL gdb.initializeEntity(e, $dim)} IN TRANSACTIONS OF 100 ROWS
				""",
				Map.of("dim", dim)
		);

		// Initialize predicate embeddings
		db.executeTransactionally(
				"""
					MATCH ()-[p]->()
					WITH type(p) AS label, p
					ORDER BY label, p.id
					WITH label, collect(p)[0] AS minP
					CALL {
						WITH minP
						CALL gdb.initializePredicate(minP, $dim)
					} IN TRANSACTIONS OF 100 ROWS
				""",
				Map.of("dim", dim)
		);

		// Get max fact id
		long maxFactId = (long) tx.execute("MATCH ()-[p]->() RETURN MAX(p.id) AS id").next().get("id");

		// Get max entity id
		long maxEntityId = (long) tx.execute("MATCH (e:Entity) RETURN MAX(e.id) AS id").next().get("id");

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

		// For each epoch
		for (int i = 0; i < epochs; i++) {
			System.out.println("Epoch " + i);

			// Normalize entities
			db.executeTransactionally("MATCH (e:Entity) CALL {WITH e CALL gdb.normalizeEntity(e)} IN TRANSACTIONS OF 100 ROWS", Map.of("dim", dim));

			// Sample batch
			System.out.println("- Sampling batch");
			long startTime = System.nanoTime();

			List<Map<String, Object>> B = new ArrayList<>();
			for (int j = 0; j < batchSize; j++) {
				// Get random fact in training split
				long fId = -1;
				while (fId == -1 || !FactExists(tx, fId)) {
					fId = random.nextLong(maxFactId + 1);
				}
				Map<String, Object> fact = tx.execute("MATCH (s:Entity)-[p {id: $fId}]->(o:Entity) RETURN s.id AS s, type(p) AS p, o.id AS o", Map.of("fId", fId)).next();
				long s = (long) fact.get("s");
				String p = fact.get("p").toString();
				long o = (long) fact.get("o");

				// Generate negative samples
				for (int k = 0; k < negativeRate; ) {
					long eId = random.nextLong(maxEntityId + 1);
					boolean corruptS = random.nextLong(100) < 50;
					if (corruptS) {
						if (!CorruptedFactExists(tx, eId, p, o)) {
							B.add(Map.of("s", s, "p", p, "o", o, "sp", eId, "op", o));
							k++;
						}
					} else {
						if (!CorruptedFactExists(tx, s, p, eId)) {
							B.add(Map.of("s", s, "p", p, "o", o, "sp", s, "op", eId));
							k++;
						}
					}
				}
			}

			long elapsedTimeNanos = System.nanoTime() - startTime;
			double elapsedTimeSeconds = (double) elapsedTimeNanos / 1_000_000_000;
			System.out.println("- Sampling took: " + elapsedTimeSeconds + "s");

			System.out.println("- Updating embeddings");
			startTime = System.nanoTime();

			// Update embeddings based on ∇L(B) using α
			for (Map<String, Object> sample : B) {
				long s = (long) sample.get("s");
				long o = (long) sample.get("o");
				long sp = (long) sample.get("sp");
				long op = (long) sample.get("op");
				String p = (String) sample.get("p");
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

			elapsedTimeNanos = System.nanoTime() - startTime;
			elapsedTimeSeconds = (double) elapsedTimeNanos / 1_000_000_000;
			System.out.println("- Updating took: " + elapsedTimeSeconds + "s");

			System.out.println("- Scaling predicate embeddings");
			startTime = System.nanoTime();

			// Scale predicate embeddings
			for (Map<String, Object> sample : B) {
				String pId = predicateIds.get((String) sample.get("p"));
				db.executeTransactionally(
						"""
                            MATCH ()-[p]->()
                            WHERE elementId(p) = $p
                            CALL gdb.scalePredicate(p)
                        """,
						Map.of("p", pId)
				);
			}

			elapsedTimeNanos = System.nanoTime() - startTime;
			elapsedTimeSeconds = (double) elapsedTimeNanos / 1_000_000_000;
			System.out.println("- Scaling took: " + elapsedTimeSeconds + "s");
		}

		tx.commit();
		tx.close();
	}

	private static boolean FactExists(Transaction tx, long id) {
		return tx.execute("MATCH ()-[p {id: $id, split: 0}]->() RETURN p", Map.of("id", id)).hasNext();
	}

	private static boolean CorruptedFactExists(Transaction tx, long sp, String p, long op) {
		return tx.execute(String.format("MATCH (sp:Entity {id: $sp})-[p:`%s` {split: 0}]->(op:Entity {id: $op}) RETURN *", p), Map.of("sp", sp, "op", op)).hasNext();
	}

	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0], jsonFile = args[1];

		String[] jsonLines = Files.readString(Path.of(jsonFile)).split("\n");
		for (String line : jsonLines) {
			JSONObject json = new JSONObject(line);
			String kg = json.getString("kg");
			int dim = json.getInt("dim");
			int batchSize = json.getInt("bs");
			int negativeRate = json.getInt("nr");
			int totalEpochs = json.getInt("epochs");
			int seed = json.getInt("seed");
			String alpha = String.valueOf(json.getDouble("alpha"));
			String gamma = String.valueOf(json.getDouble("gamma"));
			String distance = json.getString("dist");

			// Get the seed
			random.setSeed(seed);

			try (DatabaseManagementService serviceDb = getNeo4jConnection(neo4jFolder, kg);) {
				GraphDatabaseService db = serviceDb.database(GraphDatabaseSettings.initial_default_database.defaultValue());
				DependencyResolver resolver = ((GraphDatabaseAPI) db).getDependencyResolver();
				GlobalProcedures procedures = resolver.resolveDependency(GlobalProcedures.class, DependencyResolver.SelectionStrategy.SINGLE);
				procedures.registerProcedure(TransEModelTrain.class);

				// TODO Implement the training algorithm from the notes. Use BigDecimal all the
				// time and follow the reproducibility checklist. You can use the procedures
				// above, you can create your own procedures, or you can implement directly over
				// Java without procedures. The latter is probably the slowest as it requires
				// you to exchange all the embeddings back and forth. Using procedures,
				// everything happens in the database side and things will go faster.

				System.out.println("=== Processing " + kg + " ===");
				db.executeTransactionally("CREATE INDEX entity_index IF NOT EXISTS FOR (e:Entity) ON e.id");
				Train(db, dim, batchSize, negativeRate, totalEpochs, alpha, gamma, distance);
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