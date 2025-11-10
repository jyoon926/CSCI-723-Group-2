package edu.rit.gdb.a5;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;

public class RuleModelTrain {
	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0], jsonFile = args[1], resultsFolder = args[2];

		String[] jsonLines = Files.readString(Path.of(jsonFile)).split("\n");
		for (String line : jsonLines) {
			JSONObject json = new JSONObject(line);
			String kg = json.getString("kg");
			int minSup = json.getInt("min_sup");
			int maxCWAConf = json.getInt("max_cwa_conf_den");
			JSONArray headJsonArray = json.getJSONArray("head_predicates");
			JSONArray bodyJsonArray = json.getJSONArray("body_predicates");
			List<String> headPredicates = new ArrayList<>();
			List<String> bodyPredicates = new ArrayList<>();
			for (int i = 0; i < headJsonArray.length(); i++)
				headPredicates.add(headJsonArray.getString(i));
			for (int i = 0; i < bodyJsonArray.length(); i++)
				bodyPredicates.add(bodyJsonArray.getString(i));

			PrintWriter writer = new PrintWriter(resultsFolder + kg + "RuleModel.txt");

			try (DatabaseManagementService serviceDb = getNeo4jConnection(neo4jFolder, kg);) {
				GraphDatabaseService db = serviceDb.database(GraphDatabaseSettings.initial_default_database.defaultValue());

				// TODO For each head predicate, in lexicographical order, compute the six types
				// of Horn rules discussed in the notes. For each type, you can only use the
				// predicates provided in the body. There are two filters. First, minSup is the
				// minimum support, if a rule has a support less than that, the rule will not be
				// output. Second, maxCWAConf is the maximum number of pairs that we will allow
				// for the denominator of the CWA confidence. When the threshold is reached, the
				// rule should be pruned.

				System.out.printf("=== Processing %s ===%n", kg);
			}

			writer.close();
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
