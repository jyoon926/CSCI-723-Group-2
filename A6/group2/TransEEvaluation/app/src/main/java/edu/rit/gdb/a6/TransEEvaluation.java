package edu.rit.gdb.a6;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

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
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

public class TransEEvaluation {

	public TransEEvaluation() {
		super();
	}

	@UserFunction(name = "gdb.distance")
	public String distance(@Name("s") Node s, @Name("p") Relationship p, @Name("o") Node o,
			@Name("distance") String distanceStr) {
		return "";
	}

	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0], jsonFile = args[1];
		String[] jsonLines = Files.readString(Path.of(jsonFile)).split("\n");
		for (String line : jsonLines) {
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

				// TODO For each fact in each split, you must compute eight numbers. They are
				// the rank of the fact and total number of negatives according to different
				// corruptions and negative identification.
				// - Corrupting the subject using sensical negatives: ranks_sensical, r.totals_sensical
				// - Corrupting the object using sensical negatives: ranko_sensical, r.totalo_sensical
				// - Corrupting the subject using nonsensical negatives: ranks_nonsensical, r.totals_nonsensical
				// - Corrupting the object using nonsensical negatives: ranko_nonsensical, r.totalo_nonsensical
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
