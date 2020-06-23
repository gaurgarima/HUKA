package maintenance.deletion;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import main.ConfigurationReader;
import main.GlobalDSReader;


public class DeletionHandler {

	/**
	 * @param args
	 * @throws IOException 
	 */
	//static int bit=0;
	List<String> queryList;		//List of queries 
	 HashMap<String,HashMap<String, ArrayList<String>>> invertedIndex=new HashMap();
	 HashMap<String,HashSet<String>> edgeToCPMap = new HashMap();
	

	String invertedIndexfilePath = ConfigurationReader.get("INVERTED_INDEX");
	String edgeToCPFile = ConfigurationReader.get("EDGE_TO_CP_MAP");
	String kgPath = ConfigurationReader.get("GRAPH_DATABASE");
	
	//GraphDatabaseFactory graphDbFactory ;
	GraphDatabaseService db;

	public DeletionHandler(String database) {
		
		try{

			invertedIndex.putAll(GlobalDSReader.getInvertedIndex());
			edgeToCPMap.putAll(GlobalDSReader.getEdgeToCPMap());
			db = GlobalDSReader.getDBInstance();
			      
        }catch(Exception e){}
		
	}	
	
	
	public String delete (String edgeId) throws IOException {
	
		/*
		 * Find the affected queries
		 */
		
		long st, et;

		st = System.nanoTime();
		edgeId = "e".concat(edgeId);
		
		if (invertedIndex.containsKey(edgeId)) {
		
			List<String> candidateQuerySet = new ArrayList();
			candidateQuerySet.addAll(invertedIndex.get(edgeId).keySet());
			UpdatePropagator propagator = new UpdatePropagator(candidateQuerySet);	
			HashMap<String, Integer> affectedQueryMap = new HashMap();
			affectedQueryMap.putAll(propagator.deleteEdge(edgeId, invertedIndex.get(edgeId)));
			et = System.nanoTime();
			
		} else {
		
			et = System.nanoTime();
		}
		
		return Long.toString(et-st);
		
	}
	
	public String update(String edgeId) throws IOException {
		
		long stCpUpdate, etCpUpdate;
		long stIndexUpdate, etIndexUpdate;
		
		stCpUpdate = System.nanoTime();
		updateCPWithNeo(edgeId);
		etCpUpdate = System.nanoTime();
		
		stIndexUpdate = System.nanoTime();
		updateIndexes(edgeId);
		etIndexUpdate = System.nanoTime();

		return Long.toString(etCpUpdate-stCpUpdate)+"\t"+Long.toString(etIndexUpdate-stIndexUpdate);
	}
	

	private boolean updateCPWithNeo(String edgeId) {
		
		boolean status = true;
		
		HashSet<String> affectedCP = new HashSet<String>();
		
		/*
		 * Fetch value from the edeToCPMap index
		 */
		if (edgeToCPMap.containsKey(edgeId)) {
			
			affectedCP.addAll(edgeToCPMap.get(edgeId));			
		} else {
			return false;
		}
		
		String strComp[];
		String cpVertexId, expectedRelation;
		HashMap<String, HashSet<String>> cpToExpectedRelation = new HashMap();
		
		/*
		 * Find the involved expectedRelations so as to avoid multiple access to same value
		 */
		for (String str: affectedCP) {
			
			strComp = str.split(" : ");
			cpVertexId = strComp[0];
			expectedRelation = strComp[1];
			
			if (cpToExpectedRelation.containsKey(cpVertexId)) {
				cpToExpectedRelation.get(cpVertexId).add(expectedRelation);
			
			} else {
				HashSet<String> t = new HashSet();
				t.add(expectedRelation);
				cpToExpectedRelation.put(cpVertexId, t);
			}
		}
		
		/*
		 * Update actual CP values
		 */
		
		HashMap<Long , HashMap<String, String>> allCPTagMap = new HashMap();
		allCPTagMap.putAll(readTag(cpToExpectedRelation.keySet()));
		HashMap<Long , HashMap<String, String>> updatedCPTag = new HashMap();
		
		for (Entry<String, HashSet<String>> entry: cpToExpectedRelation.entrySet()) {
		
			HashSet<String> expectedRelationSet = new HashSet();
			expectedRelationSet.addAll(entry.getValue());
			Long vId = Long.parseLong(entry.getKey());
			HashMap<String, String> cpOldTag = new HashMap();
			cpOldTag.putAll(allCPTagMap.get(vId));
			
			String oldTag = "";
			String newTag = "";
			
			for (String relation: expectedRelationSet) {
			
				oldTag = cpOldTag.get(relation);
				String items[] = oldTag.split("; ");
				newTag = "";
				
				for (int i = 0; i < items.length; i++) {
					
					
					if (items[i].length() == 0)
						continue;
					
					String itemComp[] = items[i].split(" \\| ");
					String poly = itemComp[3];
					String newPoly = "";
					boolean isChanged = true;
			
					String addends[] = poly.split("\\+");
					ArrayList<String> temp = new ArrayList();

					for (String addend: addends) {
			
						ArrayList terms = new ArrayList(Arrays.asList(addend.split("e")));
						temp.clear();
						temp.addAll(terms);
						terms.clear();

						for(String t: temp) {
							if(t.endsWith("."))
								t = t.substring(0,t.length()-1);
								
							terms.add(t);
						}
									
						if(terms.contains(edgeId)) {
										
						} else {
							newPoly = newPoly.concat(addend+"+");										
						}
					}
							
					if (newPoly.contains("+")) {
						newPoly = newPoly.substring(0, newPoly.lastIndexOf("+"));
					}
			
			if(newPoly.length() == 0) {
				newPoly = poly;
				isChanged = false;
			}
					
			if (!isChanged) {
				newTag = newTag.concat(items[i]+"; ");
					
			} else if (newPoly.length() > 1) {
				String temp1 = itemComp[0]+" | "+itemComp[1]+" | "+itemComp[2]+" | "+newPoly+"; ";
				newTag = newTag.concat(temp1);
			} 
					
		} // ending items
				
		if(newTag.length() > 0)
			cpOldTag.put(relation, newTag);
				
				
	}
			
		updatedCPTag.put(vId, cpOldTag);
			
	}
		
	updateTag(updatedCPTag);
	return status;
}

	
	private HashMap<Long, HashMap<String, String>> readTag( Set<String> vIdSet) {

		HashMap<Long, HashMap<String, String>> tagMap = new HashMap();
		
		/*
		 * Get vertex and get its "tag" property
		 */
		String tag = "";
		
		try(Transaction tx = db.beginTx()) {
			
			for (String vID: vIdSet) {

				HashMap<String, String> relationMap = new HashMap();
				
				long id = Long.parseLong(vID);
				Node node = db.getNodeById(id);
				boolean hasTag = true;
				
				if(node.hasProperty("tag")) {
					tag = (String) node.getProperty("tag");
				} else {
					 tagMap.put(id, relationMap);
					 hasTag=  false;
				}

				if (hasTag) {					
				String tagComp[] = tag.split(" RELATION__");
				
				for(String str: tagComp) {

					if(str.length()>0) {
						String strComp[] = str.split("->");
						relationMap.put(strComp[0],strComp[1]);
					}
				}
		
				tagMap.put(id,relationMap);
			 }			
			}
			
			tx.success();
			tx.close();
		}
		
		return tagMap;
	}


	private void updateTag(HashMap<Long ,HashMap<String, String>> tagRelationMap) {
		
		HashMap<String, String> relationMap = new HashMap();
		/*
		 * Get vertex and get its "tag" property
		 */
		String tag = "";

		try(Transaction tx = db.beginTx()) {
			
		for (Entry<Long, HashMap<String, String>> entry: tagRelationMap.entrySet()) {
			
			tag = "";
			for(String relation : entry.getValue().keySet()) {
				
				tag = tag.concat(" RELATION__"+relation+"->"+entry.getValue().get(relation));
			}
			
			Node node = db.getNodeById(entry.getKey());
			node.setProperty("tag",tag);
	
			}
				
		tx.success();
		tx.close();
		}		
	}
	
	
	private void updateIndexes(String edgeId) throws IOException {
		
		HashSet<String> map = new HashSet();
		map.add(edgeId);
		
		GlobalDSReader.removeInvertedIndex(map);
		GlobalDSReader.removeEdgeToCPMap(map);
	}

	
}
