package edu.rit.gdb.a6;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipFile;

import org.json.JSONArray;
import org.json.JSONObject;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;

public class RuleBasedEvaluation {

	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0], ruleModelsZipFile = args[1], jsonFile = args[2];

		String[] jsonLines = Files.readString(Path.of(jsonFile)).split("\n");
		for (String line : jsonLines) {
			JSONObject json = new JSONObject(line);
			String kg = json.getString("kg");
			String kgValid = json.getString("kg_valid");
			String kgTest = json.getString("kg_test");
			JSONArray predicatesArray = json.getJSONArray("predicates");
			List<String> predicates = new ArrayList<>();
			for (int i = 0; i < predicatesArray.length(); i++)
				predicates.add(predicatesArray.getString(i));

			try (DatabaseManagementService serviceDb = getNeo4jConnection(neo4jFolder, kg);
					DatabaseManagementService serviceDbValid = getNeo4jConnection(neo4jFolder, kgValid);
					DatabaseManagementService serviceDbTest = getNeo4jConnection(neo4jFolder, kgTest);
					ZipFile zipFile = new ZipFile(new File(ruleModelsZipFile));) {
				GraphDatabaseService db = serviceDb
						.database(GraphDatabaseSettings.initial_default_database.defaultValue()),
						dbValid = serviceDbValid
								.database(GraphDatabaseSettings.initial_default_database.defaultValue()),
						dbTest = serviceDbTest.database(GraphDatabaseSettings.initial_default_database.defaultValue());

				BufferedReader reader = new BufferedReader(new InputStreamReader(
						zipFile.getInputStream(zipFile.getEntry(kg + "RuleModelFull.txt")), StandardCharsets.UTF_8));

				// TODO Parse rules and keep only those whose head is in predicates.

				reader.close();

				// TODO For each split, implement Algorithm 2 in the notes. Notice that ConfS =
				// support_num/pca_confs_den and ConfO = support_num/pca_confo_den. To compute
				// these numbers, you must use BigDecimal (DECIMAL128). The annotated facts must
				// have two properties, pcas and pcao, that can be null. If they are not null,
				// they must be a list of strings with the ConfS or ConfO values computed for
				// that particular fact. The order of the values must be in the same order as
				// the rules are found in the rule file.
			} catch (Exception oops) {
				System.out.println("Dang it!");
				oops.printStackTrace();
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
