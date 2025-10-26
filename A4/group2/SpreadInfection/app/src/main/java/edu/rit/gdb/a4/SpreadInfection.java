package edu.rit.gdb.a4;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;

import org.bson.Document;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;

public class SpreadInfection {

	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0], jsonFile = args[1];

		// Read the JSON file.
		Scanner scan = new Scanner(new File(jsonFile));
		ArrayList<String> jsonLines = new ArrayList<>();
		while(scan.hasNextLine()){
			jsonLines.add(scan.nextLine());
		}
		scan.close();

		// For each test case
		for (int input = 0; input < jsonLines.size(); input++){
			Document doc = Document.parse(jsonLines.get(input));
			String database = doc.getString("database");
			final int repetitions = doc.getInteger("repetitions");
			final List<Integer> seeds = doc.getList("seeds", Integer.class);
			final double infectionRate = doc.getDouble("infection_rate");

			try (DatabaseManagementService serviceDb = getNeo4jConnection(neo4jFolder, database);) {
				GraphDatabaseService db = serviceDb.database(GraphDatabaseSettings.initial_default_database.defaultValue());

				// For each seed, you must infect it using the status property. You need to
				// spread the infection using the infection rate. You need to repeat this
				// process a couple of times. At the end, you need to measure how many nodes
				// were infected and update the f property of the seed with that value. It is
				// expected that nodes that belong to the degeneracy core will have larger
				// infection counts than nodes that are not in the degeneracy core.

				for (long v : seeds) {
					// Set v.f = 0
					db.executeTransactionally("MATCH (v:Node {id: $v}) SET v.f = 0", Map.of("v", v));
					for (int i = 0; i < repetitions; i++) {
						// set all nodes to Susceptible (null)
						db.executeTransactionally("MATCH (vp:Node) CALL {WITH vp REMOVE vp.status} IN TRANSACTIONS OF 1000 ROWS");
						// set v.status = Infected
						db.executeTransactionally("MATCH (v:Node {id: $v}) SET v.status = \"I\"", Map.of("v", v));

						int fp = 0;
						int step = 0;

						while (InfectedNodesLeft(db)) {
							int count = db.executeTransactionally(
									"""
										MATCH (vp:Node)
										WHERE vp.status = "I"
										SET vp.status = "R"
										WITH vp
										MATCH (vp)--(vpp:Node)
										WHERE vpp.status IS NULL
										WITH vpp, rand() AS r
										WHERE $beta >= r
										SET vpp.status = "I"
										RETURN COUNT(vpp) AS count
									""",
									Map.of("beta", infectionRate),
									r -> ((Long) r.next().get("count")).intValue()
							);
							fp += count;
							step += 1;
						}
						db.executeTransactionally(
								"""
									MATCH (v:Node {id: $v})
									SET v.f = v.f + ($fp / $step)
								""",
								Map.of("v", v, "fp", fp, "step", step)
						);
					}
					db.executeTransactionally(
							"""
                                MATCH (v:Node {id: $v})
                                SET v.f = v.f / $r
                            """,
							Map.of("v", v, "r", repetitions)
					);
				}
			}
		}
	}

	private static boolean InfectedNodesLeft(GraphDatabaseService db) {
		return db.executeTransactionally(
				"""
					MATCH (vp:Node)
					WHERE vp.status = "I"
					RETURN COUNT(vp) AS count
				""",
				Map.of(),
				r -> r.hasNext() && (long) r.next().get("count") > 0
		);
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
