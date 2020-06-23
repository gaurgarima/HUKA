package main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import preprocessing.queryRegistry.annotator.SubQuery;

public class ConfigurationReader {

	
	// Metadata Location
	
	private static String mode;
	private static String databsePath;
	private static String queryResultPath;
	private static String subqueryResultPath;
	private static String localMergerdPlanPath;
	private static String globalExecutionPlan;
	private static String invertedIndexPath;
	private static String cpMapPath;
	private static String edgeToCPMapPath;
	private static String estimatingModelPath;
	
	
	private static String database;
	private static String dbBackend;

	private static String jenaVirtDatastore;
		
	private static String sqlDBUsername;
	private static String sqlDBPassword;
	
	private static Long lastVertexId;
	private static Long lastEdgeUserId;
	
	public static HashMap<String,HashMap<String,ArrayList<String>>> invertedIndex = new HashMap();
	public static HashMap<String, HashMap<String, ArrayList<String>>> cpToMetadataMap;
	public static HashMap<String, SubQuery> subQueryMap;
	public static HashMap<String,HashSet<String>> edgeToCPMap = new HashMap();

	private static HashMap<String, String> configMap;
	
	public static void readConfiguration() throws IOException {
		/*
		 * sqlDataSetPath
		 * sqlUsername
		 * sqlPassword
		 * 
		 * titanDB
		 * rdfDatastore
		 * 
		 * 
		 * 
		 */
		mode = "E";	// T: testing, E: experiment
		configMap = new HashMap();
		
		BufferedReader br = new BufferedReader(new FileReader(new File("./config.txt")));
	
		String line;
		//SQL_PASSWD	
		set("SQL_PASSWD","");
		
		while ((line = br.readLine()) != null) {
			String lineComp[] = line.split("\t");
			
			if(lineComp[1].contains("DATASET"))
				lineComp[1] = lineComp[1].replaceAll("DATASET", configMap.get("DATASET"));
			

			if(lineComp[1].contains("CONFIG"))
				lineComp[1] = lineComp[1].replaceAll("CONFIG", configMap.get("CONFIG"));
			
			set(lineComp[0], lineComp[1]);
		}
		
		br.close();
		
	
	}	
	
	public static String getMode() {
		return mode;
	}
	
	public static String get(String key) { 
		String val = configMap.get(key);
		/*
		if (val.contains("DATASET")) {
			val = val.replaceAll("DATASET", configMap.get("DATASET"));
		}
		*/
		return val;
	}
	
	private static void set (String key, String val) {
		configMap.put(key, val);
	}
}
