package edu.rit.gdb.a6;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipFile;

import org.json.JSONArray;
import org.json.JSONObject;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;

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
				List<Map<String, Object>> rules = new ArrayList<>();
				while ((line = reader.readLine()) != null) {
					Map<String, Object> ruleMap = new HashMap<>();
					json = new JSONObject(line);
					String rule = json.getString("rule");
					ruleMap.put("rule", rule);
					String predicate = rule.substring(1, rule.indexOf("`", 1));
					if (predicates.contains(predicate)) {
						ruleMap.put("predicate", predicate);
						ruleMap.put("support_num", json.getInt("support_num"));
						ruleMap.put("cwa_conf_den", json.getInt("cwa_conf_den"));
						ruleMap.put("pca_confs_den", json.getInt("pca_confs_den"));
						ruleMap.put("pca_confo_den", json.getInt("pca_confo_den"));
						BigDecimal supportNum = new BigDecimal(String.valueOf(ruleMap.get("support_num")));
						BigDecimal confsDen = new BigDecimal(String.valueOf(ruleMap.get("pca_confs_den")));
						BigDecimal confoDen = new BigDecimal(String.valueOf(ruleMap.get("pca_confo_den")));
						BigDecimal confs = supportNum.divide(confsDen, MathContext.DECIMAL128);
						BigDecimal confo = supportNum.divide(confoDen, MathContext.DECIMAL128);
						ruleMap.put("confs", confs.toString());
						ruleMap.put("confo", confo.toString());
						rules.add(ruleMap);
					}
				}

				reader.close();

				// TODO For each split, implement Algorithm 2 in the notes. Notice that ConfS =
				// support_num/pca_confs_den and ConfO = support_num/pca_confo_den. To compute
				// these numbers, you must use BigDecimal (DECIMAL128). The annotated facts must
				// have two properties, pcas and pcao, that can be null. If they are not null,
				// they must be a list of strings with the ConfS or ConfO values computed for
				// that particular fact. The order of the values must be in the same order as
				// the rules are found in the rule file.

				processSplit(db, dbValid, predicates, rules, 1);
				processSplit(db, dbTest, predicates, rules, 2);
			} catch (Exception oops) {
				System.out.println("Dang it!");
				oops.printStackTrace();
			}
		}
	}

	private static void processSplit(
			GraphDatabaseService db,
			GraphDatabaseService dbZ,
			List<String> predicates,
			List<Map<String, Object>> allRules,
			int split
	) {
		// Line 1
		for (String predicate : predicates) {
			System.out.println("Processing: " + predicate);

			// Lines 2-3
			Set<String> domain = getDomainOrRange(db, predicate, true, split);
			Set<String> range = getDomainOrRange(db, predicate, false, split);
			System.out.println("\tRetrieved domain and range.");

			// Line 4
			List<Map<String, Object>> rules = allRules.stream().
					filter(r -> r.get("predicate").equals(predicate)).toList();
			for (Map<String, Object> ruleMap : rules) {
				System.out.println("\tProcessing: " + ruleMap.get("rule"));
				// Line 5
				Set<Map<String, String>> PS = new HashSet<>(), PO = new HashSet<>();

				// Line 6
				String rule = (String) ruleMap.get("rule");
				for (Map<String, String> pair : getCWA(db, rule, split)) {
					String x = pair.get("x");
					String y = pair.get("y");

					// Line 7
					if (domain.contains(x) || range.contains(y)) {
						// Lines 8-10
						if (checkPCA(db, predicate, x, y, split, true)) {
							PS.add(pair);
						}
						// Lines 11-13
						if (checkPCA(db, predicate, x, y, split, false)) {
							PO.add(pair);
						}
					}
				}

				// Lines 16-18
				dbZ.executeTransactionally(
						"""
							UNWIND $pairs AS pair
							CALL {
								WITH pair
								MATCH (x), (y)
								WHERE elementId(x) = pair.x AND elementId(y) = pair.y
								MERGE (x)-[p:`%s`]->(y)
								ON CREATE SET p.pcas = [$confs]
								ON MATCH SET p.pcas = coalesce(p.pcas, []) + [$confs]
							} IN TRANSACTIONS OF 1000 ROWS;
						""".formatted(predicate),
						Map.of("pairs", PS, "confs", ruleMap.get("confs"))
				);

				// Lines 19-21
				dbZ.executeTransactionally(
						"""
							UNWIND $pairs AS pair
							CALL {
								WITH pair
								MATCH (x), (y)
								WHERE elementId(x) = pair.x AND elementId(y) = pair.y
								MERGE (x)-[p:`%s`]->(y)
								ON CREATE SET p.pcao = [$confo]
								ON MATCH SET p.pcao = coalesce(p.pcao, []) + [$confo]
							} IN TRANSACTIONS OF 1000 ROWS;
						""".formatted(predicate),
						Map.of("pairs", PO, "confo", ruleMap.get("confo"))
				);
			}
			System.out.println("Processed " + predicate);
		}
	}

	private static boolean checkPCA(GraphDatabaseService db, String predicate, String x, String y, int split, boolean isPcaS) {
		String pca = String.format(
				isPcaS ?
						"(x)-[p:`%s`]->() WHERE elementId(x) = $x" :
						"()-[p:`%s`]->(y) WHERE elementId(y) = $y",
				predicate
		);
		String cypher = """
				MATCH %s AND p.split <= $split
				RETURN p
		""".formatted(pca);
		return db.executeTransactionally(cypher, Map.of("x", x, "y", y, "split", split), Result::hasNext);
	}

	private static List<Map<String, String>> getCWA(GraphDatabaseService db, String rule, int split) {
		// Regex for parsing `rel`(?v1, ?v2)
		Pattern pattern = Pattern.compile("`([^`]+)`\\(\\?([a-z]),\\s*\\?([a-z])\\)");

		// Get the body
		String ruleBody = rule.substring(rule.indexOf("<=") + 2).trim();

		StringBuilder cypherMatch = new StringBuilder();
		StringBuilder cypherWhere = new StringBuilder();
		int pCounter = 1;

		// Iterate through body parts (separated by " AND ")
		for (String part : ruleBody.split("\\s+AND\\s+")) {
			Matcher matcher = pattern.matcher(part);
			if (matcher.find()) {
				String relType = matcher.group(1);
				String var1 = matcher.group(2);
				String var2 = matcher.group(3);
				String relVar = "p" + pCounter++;
				if (!cypherMatch.isEmpty()) cypherMatch.append(", ");
				cypherMatch.append(String.format("(%s)-[%s:`%s`]->(%s)", var1, relVar, relType, var2));
				if (!cypherWhere.isEmpty()) cypherWhere.append(" AND ");
				cypherWhere.append(String.format("%s.split <= %d", relVar, split));
			}
		}

		// Final query
		String bodyCypher = cypherMatch.toString();
		if (!cypherWhere.isEmpty()) {
			bodyCypher += " WHERE " + cypherWhere;
		}
		String cypher = String.format("MATCH %s WITH DISTINCT x, y RETURN COLLECT({x: elementId(x), y: elementId(y)}) AS pairs", bodyCypher);
		return db.executeTransactionally(cypher, Map.of(), r -> (List<Map<String, String>>) r.next().get("pairs"));
	}

	private static Set<String> getDomainOrRange(GraphDatabaseService db, String predicate, boolean isDomain, int split) {
		String returnVar = isDomain ? "s" : "o"; // For domain, return s. For range, return o.
		String query = String.format(
			"MATCH (s)-[r:`%s`]->(o) WHERE r.split = $split WITH DISTINCT %s RETURN COLLECT(elementId(%s)) AS ids",
			predicate, returnVar, returnVar
		);
		return db.executeTransactionally(query, Map.of("split", split), r -> new HashSet<>(((List<String>) r.next().get("ids"))));
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
