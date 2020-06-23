package main;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

/*
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
*/

import preprocessing.queryRegistry.annotator.SubQuery;
import preprocessing.queryRegistry.executionPlan.global.BaseRelation;
import preprocessing.queryRegistry.executionPlan.global.InternalRelation;
import preprocessing.queryRegistry.executionPlan.local.AndOrGraph;

public class GlobalDSReader {

	/**
	 * Datastore objects: titan graph, mariadb connection
	 */
//	static TitanGraph graph;
	static Connection connect = null;
	static GraphDatabaseFactory graphDbFactory ;
	static GraphDatabaseService db ;

	//static Statement statement = null;

	static HashMap<String,HashMap<String, ArrayList<String>>> invertedIndex=new HashMap();
	static HashMap<String,HashSet<String>> edgeToCPMap = new HashMap();
	static HashMap<String, String> vertexIdMap = new HashMap();
	static HashMap<String, SubQuery> subQueryMap = new HashMap();

	static HashMap<String, BaseRelation> baseRelationMap = new HashMap();
	static HashMap<String, InternalRelation> internalRelationMap = new HashMap();
	static AndOrGraph globalGraph;

	static String invertedIndexfilePath ;
	static String edgeToCPFile;

	static String  titanBackend ;
	static String kgPath ;
	static String subqueryMapPath ;
	
	static String username ;
	static String passwrd ;

	static String baseNodeMapPath ;
	static String internalMapPath;
	static String globalGraphPath;
	static String vertexMapFile;

	static String dataset;
	static boolean isGlobalGraphChange;
	static boolean isBaseRelationChange;
	static boolean isInternalRelationChange;
	static boolean isSubQueryMapChange;

	// To handle special 1:M potential match query
	
	static HashMap<String, HashMap<String, String>> predicateToAffectedQueryMap;
	static HashMap<String, String> subqueryToInternalNodeMap;
	static HashMap<String, HashMap<String, ArrayList<String>>> queryToSameEdgeMap; 
	
	static String predicateToSpecialQueryPath ;
	static String subqueryToInternalNodePath ;
	static String queryToSameEdgePath ;

	
	public static void instantiate() throws SQLException, ClassNotFoundException, IOException {
	
		isGlobalGraphChange = false;
		isBaseRelationChange = false;
		isInternalRelationChange = false;
		isSubQueryMapChange = false;

		instantiateFilePaths();
	//	instantiateGraph();
		connectToDB();
		loadMaps();
	}


	private static void instantiateFilePaths() {
		
		  invertedIndexfilePath = ConfigurationReader.get("INVERTED_INDEX");
		  edgeToCPFile = ConfigurationReader.get("EDGE_TO_CP_MAP");
		  vertexMapFile = ConfigurationReader.get("VERTEX_MAP");
		  
		   titanBackend = ConfigurationReader.get("TITAN_BACKEND");
		  kgPath = ConfigurationReader.get("GRAPH_DATABASE");
		  subqueryMapPath = ConfigurationReader.get("SUBQUERY_OBJ_MAP");
		
		  username = ConfigurationReader.get("SQL_USERNAME");
		  passwrd = ConfigurationReader.get("SQL_PASSWD");
		  passwrd = "";

		  baseNodeMapPath = ConfigurationReader.get("BASE_NODE_MAP");
		  internalMapPath = ConfigurationReader.get("INTERNAL_NODE_MAP");
		  globalGraphPath = ConfigurationReader.get("GLOBAL_EXC_GRAPH");
		  dataset = ConfigurationReader.get("DATASET");

		  predicateToSpecialQueryPath = ConfigurationReader.get("PREDICATE_SPECIAL_QUERY_MAP");
		  subqueryToInternalNodePath = ConfigurationReader.get("SUBQUERY_INTERNAL_RELATION_MAP");
		  queryToSameEdgePath = ConfigurationReader.get("QUERY_SAME_EDGE_MAP");

		
	}

	private static void loadMaps() throws IOException, ClassNotFoundException {

		
		
		File file0 = new File(vertexMapFile);
		
		if (file0.exists()) {
			
			FileInputStream fi = new FileInputStream(file0);
			ObjectInputStream si = new ObjectInputStream(fi);
			
			vertexIdMap = (HashMap<String, String>) si.readObject();

			si.close();
		    fi.close();

		}
		
		
		
		File file = new File(invertedIndexfilePath);
		
		if (file.exists()) {
			
			FileInputStream fi = new FileInputStream(file);
			ObjectInputStream si = new ObjectInputStream(fi);
			
			invertedIndex = (HashMap<String, HashMap<String, ArrayList<String>>>) si.readObject();

			si.close();
		    fi.close();

		} else {

			invertedIndex = new HashMap();
			file.createNewFile();
		}
		 

	    File file1 = new File(edgeToCPFile);	
			 
		if (file1.exists()) {
			FileInputStream fi1 = new FileInputStream(file1);
			ObjectInputStream si1 = new ObjectInputStream(fi1);
				
			edgeToCPMap = (HashMap<String, HashSet<String>>) si1.readObject();

			si1.close();
			fi1.close();
	
		} else {
			file1.createNewFile();
		}
		
		
	    File file21 = new File(baseNodeMapPath);	
		 
		if (file21.exists()) {
			FileInputStream file2 = new FileInputStream(file21);
			ObjectInputStream object1 = new ObjectInputStream(file2);
	    
			baseRelationMap = (HashMap<String, BaseRelation>) object1.readObject();
			object1.close();
			 			
		} else {
				file21.createNewFile();
		}
		
	    File file31 = new File(internalMapPath);	
		 
		if (file31.exists()) {
			FileInputStream file3 = new FileInputStream(file31);
			ObjectInputStream object3 = new ObjectInputStream(file3);
	     
			internalRelationMap = (HashMap<String, InternalRelation>) object3.readObject();
			object3.close();

		} else {
				file31.createNewFile();
		}

		
		
	    File file4 = new File(subqueryMapPath);	
		 
		if (file4.exists()) {
			
			FileInputStream	mapFile = new FileInputStream(file4);
			ObjectInputStream mapObject = new ObjectInputStream(mapFile);
			subQueryMap =  (HashMap<String, SubQuery>) mapObject.readObject();
			mapObject.close();
		} else {
				file4.createNewFile();
		}
	
	    File file5 = new File(globalGraphPath);	
		 
		if (file5.exists()) {
			FileInputStream graph_file = new FileInputStream(file5);

			 ObjectInputStream graph_object = new ObjectInputStream(graph_file);
			 globalGraph = (AndOrGraph) graph_object.readObject();
			 graph_object.close();
			
		} else {
				file5.createNewFile();
		}		

		
	    File file6 = new File(predicateToSpecialQueryPath);	
		 
		if (file6.exists()) {
			FileInputStream graph_file = new FileInputStream(file6);

			 ObjectInputStream graph_object = new ObjectInputStream(graph_file);
			 predicateToAffectedQueryMap = (HashMap<String, HashMap<String, String>>) graph_object.readObject();
			 graph_object.close();
			
		}
		
		
	    File file7 = new File(subqueryToInternalNodePath);	
		 
		if (file7.exists()) {
			FileInputStream graph_file = new FileInputStream(file7);

			 ObjectInputStream graph_object = new ObjectInputStream(graph_file);
			 subqueryToInternalNodeMap = (HashMap<String, String>) graph_object.readObject();
			 graph_object.close();
			
		}		
		

	    File file8 = new File(queryToSameEdgePath);	
		 
		if (file8.exists()) {
			FileInputStream graph_file = new FileInputStream(file8);

			 ObjectInputStream graph_object = new ObjectInputStream(graph_file);
			 queryToSameEdgeMap = (HashMap<String, HashMap<String, ArrayList<String>>>) graph_object.readObject();
			 graph_object.close();
			
		} 	

	}

	
	
	
	private static void connectToDB() throws SQLException {

		if (dataset.equals("dbpedia")) {
			connect = DriverManager
	                .getConnection("jdbc:mysql://localhost/DBPedia?"
	                        + "user="+username+"&password="+passwrd);
		} else {
			
        connect = DriverManager
                .getConnection("jdbc:mysql://localhost/"+dataset.toUpperCase()+"?"
                        + "user="+username+"&password="+passwrd);
		}
		connect.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

           // Statements allow to issue SQL queries to the database

		graphDbFactory = new GraphDatabaseFactory();
		db = graphDbFactory.newEmbeddedDatabase(new File("./meta/"+dataset+"/kg/db/neo4j/dbpedia/"));

		
	}

/*	private static void instantiateGraph() {
		
		graph = TitanFactory.open(titanBackend+":"+kgPath);	
		
	}		
	*/
	/**
	 *  Methods of indexes
	 * @return
	 */
	public static HashMap<String,HashMap<String, ArrayList<String>>> getInvertedIndex() {
		return invertedIndex;
	}

	public static void addToInvertedIndex(HashMap<String,HashMap<String, ArrayList<String>>> map) {
		invertedIndex.putAll(map);
	}
	
	public static String getVertexId(String label) {
		return vertexIdMap.get(label);
	}
	
 	public static void removeInvertedIndex(HashSet<String> set) {
		
		for (String edgeId: set) {
			
			if (invertedIndex.containsKey("e"+edgeId)) {
				invertedIndex.remove("e"+edgeId);
			}
		}
		
	}


	public static HashMap<String,HashSet<String>> getEdgeToCPMap() {
		return edgeToCPMap;
	}

	public static void addToEdgeToCPMap(HashMap<String,HashSet<String>> map) {
		edgeToCPMap.putAll(map);
	}
	
	public static void removeEdgeToCPMap(HashSet<String> set) {
		
		for (String edgeId: set) {
			
			if (edgeToCPMap.containsKey(edgeId)) {
				edgeToCPMap.remove(edgeId);
			}
		}
		
	}

	public static void addPredicateToAffectedQueryMap( HashMap<String, HashMap<String, String>> map) {
		
		
		for (String pred: map.keySet()) {
			
			if(predicateToAffectedQueryMap.containsKey(pred))
				predicateToAffectedQueryMap.get(pred).putAll(map.get(pred));
			else {
				predicateToAffectedQueryMap.put(pred, map.get(pred));
			}
		}
		
		//predicateToAffectedQueryMap.putAll(map);
	}

	public static void addqueryToSameEdgeyMap( HashMap<String, HashMap<String, ArrayList<String>>> map) {
		
		
		for (String query: map.keySet()) {
			
			if(queryToSameEdgeMap.containsKey(query))
				queryToSameEdgeMap.get(query).putAll(map.get(query));
			else {
				queryToSameEdgeMap.put(query, map.get(query));
			}
		}
		
		//predicateToAffectedQueryMap.putAll(map);
	}

	public static void addsubqueryToInternalNodeyMap( HashMap<String, String> map) {
		
		for (String subqueryId: map.keySet()) {
			subqueryToInternalNodeMap.put(subqueryId, map.get(subqueryId));
		}
		
		//predicateToAffectedQueryMap.putAll(map);
	}

	/**
	 * Metadata maps
	 * @return
	 */
	
	
	public static HashMap<String, SubQuery> getSubQueryMap() {
		return subQueryMap;
	}

	public static HashMap<String, BaseRelation> getBaseRelationMap() {
		return baseRelationMap;
	}
	
	public static HashMap<String, InternalRelation> getInternalRelationMap () {
		return internalRelationMap;
	}
	
	public static HashMap<String, HashMap<String, String>> getpredicateToAffectedQueryMap() {
		return predicateToAffectedQueryMap;
	}
	
	public static HashMap<String, String> getpredicateToAffectedQueryMap(String predicate) {
		return predicateToAffectedQueryMap.get(predicate);
	}
	
	public static boolean isPredicateSpecial(String pred) {
		return predicateToAffectedQueryMap.containsKey(pred);
	}
	
	public static HashMap<String, String> getSubqueryToInternalNodeMap() {
		return subqueryToInternalNodeMap;
	}

	public static HashMap<String, HashMap<String, ArrayList<String>>> getQueryToSameEdge() {
		return queryToSameEdgeMap;
	}

	/**
	 * Titan Methods
	 */
	
/*	public static  TitanTransaction getTitanTransaction () {
		return graph.newTransaction();
	}
*/	
	/*
	 * Get statement from dbConnection
	 */
	
 	public static Statement getSQLStatement() throws SQLException {
		return  connect.createStatement();
	}
	
	public static PreparedStatement getSQLPreparedStatement(
			String str) throws SQLException {

		return connect.prepareStatement(str);
	}
	
	public static GraphDatabaseService getDBInstance() {
		return db;
	}

	/*
	 * Changes to persistent storage
	 */
	
	public static void saveInvertedIndex() throws IOException {
		
		File file = new File(invertedIndexfilePath); // TODO: file name
		FileOutputStream fi1 = new FileOutputStream(file);
		ObjectOutputStream si1 = new ObjectOutputStream(fi1);
			
		si1.writeObject(invertedIndex);
		si1.close();
		fi1.close();

	}
	
	
	public static void saveEdgeToCPMap() throws IOException {
		
		File file2 = new File(edgeToCPFile);	
		FileOutputStream fi2 = new FileOutputStream(file2);
		ObjectOutputStream si2 = new ObjectOutputStream(fi2);
		
		si2.writeObject(edgeToCPMap);
		si2.close();
	    fi2.close();

	}
	
	public static void saveSubQueryMap() throws IOException {
		
		File file = new File(subqueryMapPath); // TODO: file name
		FileOutputStream fi1 = new FileOutputStream(file);
		ObjectOutputStream si1 = new ObjectOutputStream(fi1);
			
		si1.writeObject(subQueryMap);
		si1.close();
		fi1.close();

	}
	/*
	 * Methods to close db connections
	 */
	
/*	public static void closeTitanGraph() {
		graph.close();
	}
	*/
	
	public static void closeSQLConnection() throws SQLException {
		connect.close();
	}

	public static void closeNeoConnection() throws SQLException {
		db.shutdown();
	}




	public static void close() throws IOException, SQLException {

		saveInvertedIndex();
		saveEdgeToCPMap();
	//	saveMaps();
	//	closeTitanGraph();
		closeSQLConnection();
		closeNeoConnection();
		
	}


	public static void saveMaps() throws IOException {

		if (isGlobalGraphChange) {
			
			FileOutputStream graph_file = new FileOutputStream(globalGraphPath);
			ObjectOutputStream graph_object = new ObjectOutputStream(graph_file);
			graph_object.writeObject(globalGraph);
			graph_object.close();
			isGlobalGraphChange = false;
			
		}
		
		if (isBaseRelationChange) {
			FileOutputStream file2 = new FileOutputStream(baseNodeMapPath);
			ObjectOutputStream object1 = new ObjectOutputStream(file2);
	    
			object1.writeObject(baseRelationMap);
			object1.close();
			isBaseRelationChange = false;
		}
		
		if (isInternalRelationChange) {
			FileOutputStream file3 = new FileOutputStream(internalMapPath);
			ObjectOutputStream object3 = new ObjectOutputStream(file3);
			
			object3.writeObject(internalRelationMap);
			object3.close();
			isInternalRelationChange = false;
		}
		
		if (isSubQueryMapChange) {
		
			FileOutputStream	mapFile = new FileOutputStream(new File(subqueryMapPath));
			ObjectOutputStream mapObject = new ObjectOutputStream(mapFile);
			mapObject.writeObject(subQueryMap);
			mapObject.close();
			isSubQueryMapChange = false;
		}

	}


	public static AndOrGraph getGlobalGraph() {
		return globalGraph;
	}
	
	public static void updateGlobalGraph(AndOrGraph newGlobalGraph) {
		globalGraph = new AndOrGraph();
		globalGraph = newGlobalGraph;
		isGlobalGraphChange = true;
	}


	public static void updateBaseRelationMap(
			HashMap<String, BaseRelation> baseRelationMap2) {

		baseRelationMap = new HashMap();
		baseRelationMap.putAll(baseRelationMap2);
		isBaseRelationChange = true;
	}


	public static void updateInternalRelationMap(
			HashMap<String, InternalRelation> internalRelationMap2) {
		
		internalRelationMap = new HashMap();
		internalRelationMap.putAll(internalRelationMap2);
		isInternalRelationChange = true;
	}
	
	public static void updateSubQueryMap (HashMap<String, SubQuery> map) {
		
		subQueryMap = new HashMap();
		subQueryMap.putAll(map);
	}

	public static void updateSubQueryMap (String qid, SubQuery squery) {
		
		//subQueryMap = new HashMap();
		subQueryMap.put(qid, squery);
	}



/*	public static GraphTraversalSource getTraversal() {
		return graph.traversal();
	}*/
	
}
