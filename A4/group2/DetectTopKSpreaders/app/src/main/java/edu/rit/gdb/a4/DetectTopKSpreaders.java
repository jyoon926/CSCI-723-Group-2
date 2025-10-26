package edu.rit.gdb.a4;

import java.io.BufferedReader;
import java.io.FileReader;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import edu.rit.gdb.BigDecimalFunctions;
import edu.rit.gdb.BigDecimalSUM;

public class DetectTopKSpreaders {

	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0], jsonFile = args[1];
		try (BufferedReader reader = new BufferedReader(new FileReader(jsonFile))) {
			String line;
			while ((line = reader.readLine()) != null) {
				JSONObject obj = new JSONObject(line);
				final String database = obj.getString("database");
				final String databaseCopy = obj.getString("database_copy");
				final int maxIter = obj.getInt("max_iterations");
				final int patience = obj.getInt("patience");
				final double epsilon = obj.getDouble("epsilon");

				try (DatabaseManagementService serviceDb = getNeo4jConnection(neo4jFolder, database);
					 DatabaseManagementService serviceDbCopy = getNeo4jConnection(neo4jFolder, databaseCopy);) {
					GraphDatabaseService db = serviceDb.database(GraphDatabaseSettings.initial_default_database.defaultValue());
					GraphDatabaseService dbCopy = serviceDbCopy.database(GraphDatabaseSettings.initial_default_database.defaultValue());

					// Registering functions...
					DependencyResolver resolver = ((GraphDatabaseAPI) dbCopy).getDependencyResolver();
					GlobalProcedures procedures = resolver.resolveDependency(GlobalProcedures.class,
							DependencyResolver.SelectionStrategy.SINGLE);

					procedures.registerFunction(BigDecimalFunctions.class);
					procedures.registerAggregationFunction(BigDecimalSUM.class);

					// Get the degeneracy core (the subset of nodes with largest psi) from the
					// original database and copy them in the copy. Then, get the induced subgraph
					// (the edges connecting them) and copy them in the copy. For each node, compute
					// property x using the centrality algorithm studied in class. These x
					// properties will be informed in the copy database, not in the original
					// database. Neo4j does not support BigDecimal. You have several functions
					// available in BigDecimalFunctions, which are already registered for us. All of
					// them take string values as input, treat them as BigDecimal values and output
					// the string representation of the BigDecimal. There is also a SUM aggregation
					// function using BigDecimal (BigDecimalSUM). If you want to use them, this is
					// an example: MATCH (v) RETURN gdb.BDSUM(gdb.multiply(v.x, v.x)); this returns
					// the summation of all the squared v.x values (v.x times v.x). You are not
					// required to use these functions. You can also deal with the numbers in your
					// Java program.

					DetectTopSpreaders(db, dbCopy, maxIter, patience, epsilon);
				}
			}
		}
	}

	public static void DetectTopSpreaders(GraphDatabaseService db, GraphDatabaseService dbCopy, int maxIter, int patience, double epsilon) {
		int count = CopyDegeneracyCore(db, dbCopy);

		// Initialize v.x
		dbCopy.executeTransactionally("MATCH (v:Node) SET v.x = $count", Map.of("count", "" + (1.0 / count)));

		// Normalize
		Normalize(dbCopy);

		for (int i = 1; i <= maxIter; i++) {
			dbCopy.executeTransactionally("MATCH (v:Node) SET v.xlast = v.x");
			dbCopy.executeTransactionally(
					"""
						MATCH (v:Node)
						WITH v
						MATCH (v)--(vPrime:Node) SET vPrime.x = gdb.add(vPrime.x, v.xlast)
					"""
			);
			Normalize(dbCopy);
			String e = dbCopy.executeTransactionally(
					"""
						MATCH (v:Node)
						RETURN gdb.BDSUM(gdb.abs(gdb.subtract(v.xlast, v.x))) AS e
					""",
					Map.of(),
					r -> r.next().get("e").toString()
			);
			int result = (new BigDecimal(e)).compareTo(BigDecimal.valueOf(count * epsilon));
			if (result < 0 && i >= patience) break;
		}
	}

	private static void Normalize(GraphDatabaseService dbCopy) {
		dbCopy.executeTransactionally(
				"""
                    MATCH (v:Node)
                    WITH gdb.BDSUM(gdb.multiply(v.x, v.x)) AS s
                    MATCH (v:Node)
                    SET v.x = gdb.divide(v.x, gdb.sqrt(s))
                """
		);
	}

	private static int CopyDegeneracyCore(GraphDatabaseService db, GraphDatabaseService dbCopy) {
		long maxPsi = db.executeTransactionally("MATCH (v:Node) RETURN MAX(v.psi) AS maxPsi", Map.of(), r -> (long) r.next().get("maxPsi"));

		// Collect nodes
		List<Long> nodes = db.executeTransactionally(
				"""
					MATCH (v:Node)
					WHERE v.psi = $maxPsi
					RETURN COLLECT(v.id) AS nodes
				""",
				Map.of("maxPsi", maxPsi),
				r -> ((List<Long>) r.next().get("nodes"))
		);
		int count = nodes.size();

		// Copy nodes
		dbCopy.executeTransactionally(
				"UNWIND $nodes AS node CREATE (v:Node {id: node})",
				Map.of("nodes", nodes)
		);
		nodes.clear();

		// Create indexes over id
		db.executeTransactionally("CREATE INDEX id_index IF NOT EXISTS FOR (n:Node) ON (n.id)");
		dbCopy.executeTransactionally("CREATE INDEX id_index IF NOT EXISTS FOR (n:Node) ON (n.id)");

		// Copy edges in batches
		int skip = 0;
		int limit = 1000;
		while (true) {
			List<Map<String, Long>> edges = db.executeTransactionally(
					"""
                        MATCH (a:Node)--(b:Node)
                        WHERE a.psi = $maxPsi AND b.psi = $maxPsi AND a.id < b.id
                        WITH a.id AS a, b.id AS b
                        ORDER BY a, b
                        SKIP $skip LIMIT $limit
                        RETURN COLLECT({a: a, b: b}) AS edges
					""",
					Map.of("maxPsi", maxPsi, "skip", skip, "limit", limit),
					r -> r.hasNext() ? (List<Map<String, Long>>) r.next().get("edges") : List.of()
			);
			if (edges.isEmpty()) break;
			dbCopy.executeTransactionally(
					"""
                        UNWIND $edges AS edge
                        MATCH (a:Node {id: edge.a}), (b:Node {id: edge.b})
                        CREATE (a)-[:Edge]->(b)
                    """,
					Map.of("edges", edges)
			);
			skip += limit;
		}

		// Return number of nodes
		return count;
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
