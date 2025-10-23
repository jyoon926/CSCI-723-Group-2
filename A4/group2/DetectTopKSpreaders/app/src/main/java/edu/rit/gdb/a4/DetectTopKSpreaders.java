package edu.rit.gdb.a4;

import java.nio.file.Path;
import java.time.Duration;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import edu.rit.gdb.BigDecimalFunctions;
import edu.rit.gdb.BigDecimalSUM;

public class DetectTopKSpreaders {

	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0], jsonFile = args[1];

		{
			String database = null, databaseCopy = null;

			final int maxIter = -1, patience = -1;
			final double epsilon = -1;

			try (DatabaseManagementService serviceDb = getNeo4jConnection(neo4jFolder, database);
					DatabaseManagementService serviceDbCopy = getNeo4jConnection(neo4jFolder, databaseCopy);) {
				GraphDatabaseService db = serviceDb
						.database(GraphDatabaseSettings.initial_default_database.defaultValue()),
						dbCopy = serviceDbCopy.database(GraphDatabaseSettings.initial_default_database.defaultValue());

				// Registering functions...
				DependencyResolver resolver = ((GraphDatabaseAPI) dbCopy).getDependencyResolver();
				GlobalProcedures procedures = resolver.resolveDependency(GlobalProcedures.class,
						DependencyResolver.SelectionStrategy.SINGLE);

				procedures.registerFunction(BigDecimalFunctions.class);
				procedures.registerAggregationFunction(BigDecimalSUM.class);

				// TODO Get the degeneracy core (the subset of nodes with largest psi) from the
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
