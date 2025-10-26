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
				GraphDatabaseService db = serviceDb
						.database(GraphDatabaseSettings.initial_default_database.defaultValue());

				db.executeTransactionally("MATCH (n:Node) CALL {WITH n REMOVE n.f} IN TRANSACTIONS OF 1000 ROWS");

				// For each seed, you must infect it using the status property. You need to
				// spread the infection using the infection rate. You need to repeat this
				// process a couple of times. At the end, you need to measure how many nodes
				// were infected and update the f property of the seed with that value. It is
				// expected that nodes that belong to the degeneracy core will have larger
				// infection counts than nodes that are not in the degeneracy core.

				// for each seed value
				for (Integer v : seeds) {
					// reset the status each time
					db.executeTransactionally("MATCH (n:Node) CALL {WITH n REMOVE n.status} IN TRANSACTIONS OF 1000 ROWS");
					long seed = (long) v;
					int f = 0; // f(v) is influence of an input node v
					for (int i = 0; i < repetitions; i++) {
						int fp = 0;
						int step = 0;
						// set v.status = Infected //TODO set to X?
						db.executeTransactionally("MATCH (v {id: $id}) SET v.status = \"I\"", Map.of("id", seed));
						// all other nodes start with status of Susceptible = null

						// get all nodes with Infected status
						List<Long> infected = db.executeTransactionally(
								"MATCH (vp) " +
										"WHERE vp.status = \"I\" " +
										"SET vp.status = \"R\" " +
										"RETURN COLLECT (vp.id) as vpids",
								Map.of("id", seed),
								r -> ((List<Long>) r.next().get("vpids")));
						for (Long id : infected) {
							int cnt = 0;
							// set all nodes with Infected status to Recovered and randomly infect neighbors
							Map<String, Object> infectParams = new HashMap<>();
							infectParams.put("id", id);
							infectParams.put("beta", infectionRate);
							List<Long> newInfected = db.executeTransactionally(
									"MATCH (vp)--(n) " +
											"WHERE vp.id = $id AND n.status IS NULL " +
											"WITH rand() as r, n as n " +
											"WHERE $beta >= rand() " +
											"SET n.status = \"I\" " +
											"RETURN COLLECT(n.id) as ids",
									infectParams,
									r -> ((List<Long>) r.next().get("ids"))
							);
							cnt += newInfected.size();
							fp = fp + cnt;
							step++;
						}
						if (step != 0) {
							f = f + (fp / step);
						}
					}
					f = f / repetitions;
					// set v.f to f value
					Map<String, Object> params = new HashMap<>();
					params.put("id", seed);
					params.put("f", f);
					db.executeTransactionally("MATCH (v {id: $id}) SET v.f = $f", params);
				}
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
