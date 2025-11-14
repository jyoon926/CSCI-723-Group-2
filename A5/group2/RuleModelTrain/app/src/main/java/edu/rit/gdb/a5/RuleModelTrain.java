package edu.rit.gdb.a5;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;

import org.json.JSONArray;
import org.json.JSONObject;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

public class RuleModelTrain {
	/**
	 * 	Calculate the measurements for a rule of format: head(?x, ?y) <= body1(?x, ?z) AND body2(?y, ?z)
	 * @return String containing rule and measurement results
	 */
	private static String computeMultiBodyRule(Transaction tx, int maxCwaConf, int minSup,
											   String headPredicate, String[] bodyPredicates, String[] bodyVars){
		if (bodyPredicates.length != 2) {
			return "";
		}

		String cwaConfQuery = String.format(
				"match (%s)-[b1:`%s` {split: 0}]->(%s), (%s)-[b2:`%s` {split: 0}]->(%s) " +
						"return elementId(x) as xid, elementId(y) as yid, elementId(z) as zid, " +
						"elementId(b1) as b1id, elementId(b2) as b2id",
				bodyVars[0], bodyPredicates[0], bodyVars[1], bodyVars[2], bodyPredicates[1], bodyVars[3]
		);

		int support=0, cwaConf=0, pcaS=0, pcaO=0;
		TreeSet<String> xyids = new TreeSet<>();

		Result res = tx.execute(cwaConfQuery);
		while (res.hasNext() && cwaConf < maxCwaConf){
			var nxt = res.next();
			String currXY = (String)nxt.get("xid") + (String)nxt.get("yid");

			if(!xyids.contains(currXY)) {
				cwaConf++;

				// support: does (x)-[p]->(y) exist?
				String miniSupportQ = "MATCH (x)-[:`" + headPredicate + "` {split: 0}]->(y) " +
						"WHERE elementId(x) = $xid AND elementId(y) = $yid RETURN x.id";
				Result r1 = tx.execute(miniSupportQ, nxt);
				if (r1.hasNext()) {
					support++;
				}
				r1.close();

				// pcaS: does (x)-[p]->() exist?
				String miniS = "MATCH (x)-[r:`" + headPredicate + "` {split: 0}]->() " +
						"WHERE elementId(x) = $xid AND elementId(r) <> $b1id AND elementId(r) <> $b2id RETURN x.id";
				Result r2 = tx.execute(miniS, nxt);
				if (r2.hasNext()) {
					pcaS++;
				}
				r2.close();

				// pcaO: does ()-[p]-(y) exist?
				String miniO = "MATCH ()-[r:`" + headPredicate + "` {split: 0}]->(y) " +
						"WHERE elementId(y) = $yid AND elementId(r) <> $b1id AND elementId(r) <> $b2id RETURN y.id";
				Result r3 = tx.execute(miniO, nxt);
				if (r3.hasNext()) {
					pcaO++;
				}
				r3.close();

				xyids.add(currXY);
			}
		}
		res.close();

		if (support >= minSup && cwaConf < maxCwaConf) {
			String rule = String.format("`%s(?x, ?y) <= `%s`(?%s, ?%s) AND `%s`(?%s, ?%s)",
					headPredicate, bodyPredicates[0], bodyVars[0], bodyVars[1], bodyPredicates[1], bodyVars[2], bodyVars[3]);
			return String.format("{\"rule\": \"%s\", \"support_num\": %d, \"cwa_conf_den\": %d, \"pca_confs_den\": %d, \"pca_confo_den\": %d}\n",
					rule, support, cwaConf, pcaS, pcaO);
		}

		return "";
	}

	/**
	 * 	Calculate the measurements for a rule of format: head(?x, ?y) <= body(?x, ?y)
	 * @return String containing rule and measurement results
	 */
	private static String computeSingleBodyRule(Transaction tx, String headPredicate, String[] bodyPredicates,
												String[] bodyVars, int maxCwaConf, int minSup){
		if (bodyPredicates.length != 1){
			return "";
		}

		int support=0, cwaConf=0, pcaS=0, pcaO=0;
		String closedConfQ = "MATCH ("+ bodyVars[0] +")-[b1:`"+ bodyPredicates[0] +"` {split: 0}]->("+ bodyVars[1] +") "
		+ " with distinct [x.id, y.id] as pairs, elementId(x) as xid, elementId(y) as yid return xid, yid";

		//CWA Confidence is super set of everything we need. Get these values and calculating from there.
		Result res = tx.execute(closedConfQ);
		while (res.hasNext() && cwaConf < maxCwaConf){
			cwaConf++;
			var nxt = res.next();

			// support: does (x)-[p]->(y) exist?
			String miniSupportQ = "MATCH (x)-[:`" + headPredicate + "` {split: 0}]->(y) " +
					"WHERE elementId(x) = $xid AND elementId(y) = $yid RETURN x.id";
			Result r1 = tx.execute(miniSupportQ, nxt);
			if (r1.hasNext()){
				support++;
			}
			r1.close();

			// pcaS: does (x)-[p]->() exist?
			String miniS = "MATCH (x)-[:`" + headPredicate + "` {split: 0}]->() " +
					"WHERE elementId(x) = $xid RETURN x.id";
			Result r2 = tx.execute(miniS, nxt);
			if (r2.hasNext()) {
				pcaS++;
			}
			r2.close();

			// pcaO: does ()-[p]-(y) exist?
			String miniO = "MATCH ()-[:`" + headPredicate + "` {split: 0}]->(y) " +
					"WHERE elementId(y) = $yid RETURN y.id";
			Result r3 = tx.execute(miniO, nxt);
			if (r3.hasNext()) {
				pcaO++;
			}
			r3.close();

		}
		res.close();

		if (support >= minSup && cwaConf < maxCwaConf) {
			String rule = String.format("`%s(?x, ?y) <= `%s`(?%s, ?%s)",
					headPredicate, bodyPredicates[0], bodyVars[0], bodyVars[1]);
			return String.format("{\"rule\": \"%s\", \"support_num\": %d, \"cwa_conf_den\": %d, \"pca_confs_den\": %d, \"pca_confo_den\": %d}\n",
					rule, support, cwaConf, pcaS, pcaO);
		}
		return "";
	}

	public static void main(String[] args) throws Exception {
		final String neo4jFolder = args[0], jsonFile = args[1], resultsFolder = args[2];

		String[] jsonLines = Files.readString(Path.of(jsonFile)).split("\n");
		for (String line : jsonLines) {
			// "{"kg":"nations", "min_sup": 5, "max_cwa_conf_den": 5000,
			//   "head_predicates": ["commonbloc1", "embassy", "treaties"],
			//   "body_predicates": ["boycottembargo", "commonbloc1", "negativebehavior", "embassy", "treaties"]}",
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
			headPredicates.sort(null);
			bodyPredicates.sort(null);

			PrintWriter writer = new PrintWriter(resultsFolder + kg + "RuleModel.txt");

			try (DatabaseManagementService serviceDb = getNeo4jConnection(neo4jFolder, kg);) {
				GraphDatabaseService db = serviceDb.database(GraphDatabaseSettings.initial_default_database.defaultValue());

				// For each head predicate, in lexicographical order, compute the six types
				// of Horn rules discussed in the notes. For each type, you can only use the
				// predicates provided in the body. There are two filters. First, minSup is the
				// minimum support, if a rule has a support less than that, the rule will not be
				// output. Second, maxCWAConf is the maximum number of pairs that we will allow
				// for the denominator of the CWA confidence. When the threshold is reached, the
				// rule should be pruned.

				System.out.printf("=== Processing %s ===%n", kg);

				Transaction tx = db.beginTx();

				for (String head : headPredicates) {
					for (String pi : bodyPredicates) {
						if (!head.equals(pi)) {
							//Rule 12: h(?x, ?y) <= pi(?x, ?y)
							writer.write(computeSingleBodyRule(tx, head, new String[]{pi}, new String[]{"x", "y"}, maxCWAConf, minSup));
						}
						//Rule 13: h(?x, ?y) <= pi(?y, ?x)
						writer.write(computeSingleBodyRule(tx, head, new String[]{pi}, new String[]{"y", "x"}, maxCWAConf, minSup));

						for (String pj : bodyPredicates) {
							//Rules 17-20
							String[][] vars = new String[][] {
									{"x", "z", "z", "y"},
									{"z", "x", "z", "y"},
									{"x", "z", "y", "z"},
									{"z", "x", "y", "z"},
							};
							for (String[] vs : vars){
								writer.write(computeMultiBodyRule(tx, maxCWAConf, minSup, head, new String[]{pi, pj}, vs));
							}
						}
					}
				}
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
