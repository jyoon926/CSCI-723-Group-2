package edu.rit.gdb.a4;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;

public class ComputeDMP {

	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0], jsonFile = args[1];
		try (BufferedReader reader = new BufferedReader(new FileReader(jsonFile))) {
			String line;
			while ((line = reader.readLine()) != null) {
				JSONObject json = new JSONObject(line);
				String database = json.getString("database");
				try (DatabaseManagementService serviceDb = getNeo4jConnection(neo4jFolder, database);) {
					GraphDatabaseService db = serviceDb.database(GraphDatabaseSettings.initial_default_database.defaultValue());

					// Nodes will have either psi or psiest set (but not both). Compute the
					// ranks of the nodes using psi or psiest, descending, and using degrees when
					// psi/psiest are tied, also descending. This is stored in a rankc property.
					// Similarly, the rankd property is the rank of the node according to its
					// degree, descending. Both rankc and rankd must be realistic ranks. Finally,
					// the dmp property is the combination of both rankc and rankd for each node.

					ComputeDMP(db);
				}
			}
		}
	}

	private static void ComputeDMP(GraphDatabaseService db) {
		db.executeTransactionally("CREATE INDEX id_idx IF NOT EXISTS FOR (n:Node) ON n.id");
		List<Map<String, Object>> nodes = db.executeTransactionally(
				"""
					MATCH (n:Node)
					WITH n, COALESCE(n.psi, n.psiest) AS psi, COUNT {(n)--()} AS degree
					ORDER BY psi DESC, degree DESC, n.id ASC
					RETURN COLLECT({id: n.id, psi: psi, degree: degree}) AS nodes
				""",
				Map.of(),
				r -> (List<Map<String, Object>>) r.next().get("nodes")
		);

		// Compute rankc and add to nodes map
		SetRank(nodes, true);

		// Compute rankd and add to nodes map
		nodes.sort((a, b) -> {
			int degreeCompare = Long.compare((long) b.get("degree"), (long) a.get("degree"));
			if (degreeCompare != 0) return degreeCompare;
			return Long.compare((long) a.get("id"), (long) b.get("id"));
		});
		SetRank(nodes, false);

		// Compute dmp
		SetDMP(nodes);

		// Update nodes
		int batchSize = 1000;
		for (int i = 0; i < nodes.size(); i += batchSize) {
			db.executeTransactionally(
					"""
						UNWIND $nodes AS node
						MATCH (n:Node {id: node.id})
						SET n.rankc = node.rankc, n.rankd = node.rankd, n.dmp = node.dmp
					""",
					Map.of("nodes", nodes.subList(i, Math.min(i + batchSize, nodes.size())))
			);
		}
	}

	private static void SetRank(List<Map<String, Object>> nodes, boolean usePsi) {
		int n = nodes.size();
		for (int i = 0; i < n;) {
			// Find ties
			int j = i + 1;
			while (j < n &&
					(!usePsi || nodes.get(j).get("psi").equals(nodes.get(i).get("psi"))) &&
					nodes.get(j).get("degree").equals(nodes.get(i).get("degree"))) {
				j++;
			}
			// fractional rank = average of positions
			double rank = (i + 1 + j) / 2.0;
			for (int k = i; k < j; k++) {
				nodes.get(k).put(usePsi ? "rankc" : "rankd", rank);
			}
			i = j;
		}
	}

	private static void SetDMP(List<Map<String, Object>> nodes) {
		for (Map<String, Object> node : nodes) {
			double dmp = Math.abs(Math.log((double) node.get("rankc")) - Math.log((double) node.get("rankd")));
			node.put("dmp", dmp);
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
