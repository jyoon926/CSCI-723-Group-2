package edu.rit.gdb.a5;

import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;

import org.json.JSONObject;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class TransEModelTrain {
	public static Random random = new Random();

	public TransEModelTrain() {
		super();
	}

	private String[] generateEmbedding(long dimension) {
		String[] embedding = new String[(int) dimension];
		for (int i = 0; i < dimension; i++) {
			BigDecimal v = (new BigDecimal((random.nextLong(100) + 1))).divide(new BigDecimal(100), MathContext.DECIMAL128);
			BigDecimal max = (new BigDecimal(6)).divide((new BigDecimal(dimension)).sqrt(MathContext.DECIMAL128), MathContext.DECIMAL128);
			v = v.multiply(max.multiply(new BigDecimal(2))).subtract(max);
			embedding[i] = v.toPlainString();
		}
		return embedding;
	}

	private String[] normalizeEmbedding(String[] embedding) {
		BigDecimal norm = new BigDecimal(0);
        for (String s : embedding) {
            BigDecimal v = new BigDecimal(s);
            norm = norm.add(v.multiply(v, MathContext.DECIMAL128));
        }
		norm = norm.sqrt(MathContext.DECIMAL128);
		for (int i = 0; i < embedding.length; i++) {
			BigDecimal v = new BigDecimal(embedding[i]);
			v = v.divide(norm, MathContext.DECIMAL128);
			embedding[i] = v.toPlainString();
		}
		return embedding;
	}

	private List<BigDecimal> toBigDecimalList(String[] strList) {
		return Arrays.stream(strList).map(BigDecimal::new).toList();
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
		String[] embedding = (String[]) x.getProperty("embedding");
		List<BigDecimal> emb = toBigDecimalList(embedding);

		// Find min and max
		BigDecimal min = emb.stream().min(BigDecimal::compareTo).orElseThrow();
		BigDecimal max = emb.stream().max(BigDecimal::compareTo).orElseThrow();
		BigDecimal range = max.subtract(min);

		int dim = embedding.length;

		BigDecimal maxVal = (new BigDecimal(6)).divide(
				(new BigDecimal(dim)).sqrt(MathContext.DECIMAL128),
				MathContext.DECIMAL128
		);

		// Min-max scale to [0,1], then to [-maxVal, maxVal]
		for (int i = 0; i < emb.size(); i++) {
			BigDecimal scaled = emb.get(i).subtract(min).divide(range, MathContext.DECIMAL128);
			scaled = scaled.multiply(maxVal.multiply(new BigDecimal(2))).subtract(maxVal);
			emb.set(i, scaled);
		}

		x.setProperty("embedding", toStringArray(emb));
	}

	@Procedure(name = "gdb.gradientDescent", mode = Mode.WRITE)
	public void gradientDescent(@Name("s") Node s, @Name("p") Relationship p, @Name("o") Node o, @Name("sp") Node sp,
			@Name("op") Node op, @Name("gamma") String gammaStr, @Name("distance") String distanceStr,
			@Name("alpha") String alphaStr) {
		List<BigDecimal> sEmb = toBigDecimalList((String[]) s.getProperty("embedding"));
		List<BigDecimal> pEmb = toBigDecimalList((String[]) p.getProperty("embedding"));
		List<BigDecimal> oEmb = toBigDecimalList((String[]) o.getProperty("embedding"));
		List<BigDecimal> spEmb = toBigDecimalList((String[]) sp.getProperty("embedding"));
		List<BigDecimal> opEmb = toBigDecimalList((String[]) op.getProperty("embedding"));
		BigDecimal alpha = new BigDecimal(alphaStr);
		BigDecimal gamma = new BigDecimal(gammaStr);

		// Compute distance for positive and negative triples
		BigDecimal dPos = BigDecimal.ZERO;
		BigDecimal dNeg = BigDecimal.ZERO;

		for (int i = 0; i < sEmb.size(); i++) {
			BigDecimal diffPos = sEmb.get(i).add(pEmb.get(i)).subtract(oEmb.get(i));
			BigDecimal diffNeg = spEmb.get(i).add(pEmb.get(i)).subtract(opEmb.get(i));

			if (distanceStr.equals("L1")) {
				dPos = dPos.add(diffPos.abs());
				dNeg = dNeg.add(diffNeg.abs());
			} else { // L2
				dPos = dPos.add(diffPos.multiply(diffPos));
				dNeg = dNeg.add(diffNeg.multiply(diffNeg));
			}
		}

		if (distanceStr.equals("L2")) {
			dPos = dPos.sqrt(MathContext.DECIMAL128);
			dNeg = dNeg.sqrt(MathContext.DECIMAL128);
		}

		if (gamma.add(dPos).subtract(dNeg).compareTo(BigDecimal.ZERO) <= 0) {
			return; // No update needed
		}

		// Update embeddings
		for (int i = 0; i < sEmb.size(); i++) {
			BigDecimal xi = BigDecimal.TWO.multiply(sEmb.get(i).add(pEmb.get(i).subtract(oEmb.get(i))));
			BigDecimal xpi = BigDecimal.TWO.multiply(spEmb.get(i).add(pEmb.get(i).subtract(opEmb.get(i))));

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
		sp.setProperty("embedding", toStringArray(spEmb));
		op.setProperty("embedding", toStringArray(opEmb));
		p.setProperty("embedding", toStringArray(pEmb));
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
			double alpha = json.getDouble("alpha");
			double gamma = json.getDouble("gamma");
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

	private static void Train(
			GraphDatabaseService db,
			int dim,
			int batchSize,
			int negativeRate,
			int epochs,
			double alpha,
			double gamma,
			String distance) {
		// Initialize entity embeddings
		db.executeTransactionally("MATCH (e:Entity {split: 0}) CALL {WITH e CALL gdb.initializeEntity(e, $dim)} IN TRANSACTIONS OF 100 ROWS", Map.of("dim", dim));

		// Initialize predicate embeddings
		db.executeTransactionally(
				"""
					MATCH ()-[p]->()
					WITH type(p) AS label, p
					ORDER BY p.id
					WITH label, collect(p)[0] AS minP
					CALL {
						WITH minP
						CALL gdb.initializePredicate(minP, $dim)
					} IN TRANSACTIONS OF 100 ROWS
				""",
				Map.of("dim", dim)
		);

		// Get max fact id
		long maxFactId = db.executeTransactionally("MATCH ()-[p]->() RETURN MAX(p.id) AS id", Map.of(), r -> (long) r.next().get("id"));

		// Get max entity id
		long maxEntityId = db.executeTransactionally("MATCH (e:Entity) RETURN MAX(e.id) AS id", Map.of(), r -> (long) r.next().get("id"));

		// For each epoch
		for (int i = 0; i < epochs; i++) {
			System.out.println("-- Epoch " + i + " --");

			// Normalize entities
			db.executeTransactionally("MATCH (e:Entity {split: 0}) CALL {WITH e CALL gdb.normalizeEntity(e)} IN TRANSACTIONS OF 100 ROWS", Map.of("dim", dim));
			List<Map<String, Object>> B = new ArrayList<>();

			// Sample batch
			for (int j = 0; j < batchSize; j++) {
				// Get random fact in training split
				long fId = -1;
				while (fId == -1 || !FactExists(db, fId)) {
					fId = random.nextLong(maxFactId + 1);
				}
				Map<String, Object> fact = db.executeTransactionally("MATCH (s:Entity)-[p {id: $fId}]->(o:Entity) RETURN s.id AS s, type(p) AS p, o.id AS o", Map.of("fId", fId), Result::next);
				long s = (long) fact.get("s");
				String p = fact.get("p").toString();
				long o = (long) fact.get("o");

				// Generate negative samples
				for (int k = 0; k < negativeRate; ) {
					long eId = random.nextLong(maxEntityId + 1);
					if (EntityExists(db, eId)) {
						boolean corruptS = random.nextLong(100) < 50;
						if (corruptS) {
							boolean exists = db.executeTransactionally(String.format("MATCH (e:Entity {id: $eId})-[p:`%s` {split: 0}]->(o:Entity {id: $oId}) RETURN *", p), Map.of("eId", eId, "oId", o), Result::hasNext);
							if (!exists) {
								B.add(Map.of("s", s, "p", p, "o", o, "sp", eId, "op", o));
								k++;
							}
						} else {
							boolean exists = db.executeTransactionally(String.format("MATCH (s:Entity {id: $sId})-[p:`%s` {split: 0}]->(e:Entity {id: $eId}) RETURN *", p), Map.of("sId", s, "eId", eId, "fId", fId), Result::hasNext);
							if (!exists) {
								B.add(Map.of("s", s, "p", p, "o", o, "sp", s, "op", eId));
								k++;
							}
						}
					}
				}
			}

			// Update embeddings based on ∇L(B) using α
			for (Map<String, Object> sample : B) {
				db.executeTransactionally(
						"""
                            MATCH ()-[p:`%s`]->()
                            WITH p
                            ORDER BY p.id
                            LIMIT 1
                            MATCH (s:Entity {id: $s})-[r:`%s` {split: 0}]->(o:Entity {id: $o})
                            MATCH (sp:Entity {id: $sp})
                            MATCH (op:Entity {id: $op})
                            CALL gdb.gradientDescent(s, p, o, sp, op, $gamma, $dist, $alpha)
                        """.formatted(sample.get("p"), sample.get("p")),
						Map.of("s", sample.get("s"), "o", sample.get("o"), "sp", sample.get("sp"), "op", sample.get("op"), "gamma", gamma, "dist", distance, "alpha", alpha)
				);
			}

			// Scale predicate embeddings
			for (Map<String, Object> sample : B) {
				db.executeTransactionally(
						"""
                            MATCH ()-[p:`%s`]->()
                            WITH p
                            ORDER BY p.id
                            LIMIT 1
                            CALL gdb.scalePredicate(p)
                        """.formatted(sample.get("p"))
				);
			}

			// TODO: Evaluate early stopping based on validation split
		}
	}

	private static boolean FactExists(GraphDatabaseService db, long id) {
		return db.executeTransactionally("MATCH ()-[p {id: $id, split: 0}]->() RETURN p", Map.of("id", id), Result::hasNext);
	}

	private static boolean EntityExists(GraphDatabaseService db, long id) {
		return db.executeTransactionally("MATCH (e {id: $id, split: 0}) RETURN e", Map.of("id", id), Result::hasNext);
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