package preprocessing.queryRegistry.annotator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import main.ConfigurationReader;
import main.GlobalDSReader;

public class CPAnnotator {

	static HashMap<String, HashMap<String, ArrayList<String>>>cpToMetadataMap;
	static HashMap<String, SubQuery> subQueryMap;
	static String dataset;
	
	GraphDatabaseService db;

	public CPAnnotator(HashMap<String, SubQuery> smap) throws IOException {
		
		dataset = ConfigurationReader.get("DATASET");
		cpToMetadataMap = new HashMap();
		subQueryMap = new HashMap();
		subQueryMap.putAll(smap);
		db = GlobalDSReader.getDBInstance();
		
	}
		
	

	public static void buildCPMap(HashMap<String, HashMap<String, String>> subqueryResult) {
		
		cpToMetadataMap = new HashMap<String, HashMap<String, ArrayList<String>>>();
		String resultFileBasePath = ConfigurationReader.get("SUBQUERY_RESULT");

		String cp1Variable = new String();
		String cp2Variable = new String();
		String resultVariable = new String();
		String expectedRelation = new String();
		String direction = new String();
		
		for (Entry<String, SubQuery> entry : subQueryMap.entrySet()) {
			
			String queryId = entry.getKey();
			SubQuery subquery = entry.getValue();			
			cp1Variable = subquery.getCP1Variable();
			cp2Variable = subquery.getCP2Variable();
			resultVariable = subquery.getResultVariable();
			expectedRelation = subquery.getExpectedRelation();
			direction = subquery.getDirection();
							
			HashMap<String, String> resultList = new HashMap();
			resultList.putAll(subqueryResult.get(queryId));
			
			String lineComp[];
			String result = new String();
			String vertexInfo = new String();
			String provPoly =new String();
			String resultItem = new String();
			String cpVertexId = new String();
				
			for (Entry<String, String> entry1: resultList.entrySet()) {
	
				lineComp = entry1.getKey().split("\t");
				result = lineComp[0];
				vertexInfo = lineComp[1];
				provPoly = entry1.getValue();
					
				String resultVarComp[] = resultVariable.split(", ");
					
				if (resultVarComp.length == 1) {
					resultItem = resultVarComp[0]+" = "+result;

				} else {
						resultItem = result;
				}
					
				if (!cp1Variable.equals("NULL") && resultVariable.contains(cp1Variable)) {
	
					String vertexInfoComp[] = vertexInfo.split(cp1Variable+"=");
					String temp = vertexInfoComp[1];						
					temp = temp.substring(temp.indexOf("[")+1, temp.indexOf("]"));
					cpVertexId = temp;
				
					/*
					 * Add to the cp map	
					 */
					
					if (cpToMetadataMap.containsKey(cpVertexId)) {
							
						HashMap<String, ArrayList<String>> tempMap = new HashMap();
						tempMap.putAll(cpToMetadataMap.get(cpVertexId));
						
						if (tempMap.containsKey(expectedRelation)) {
				
							ArrayList<String> tempList = new ArrayList();
							tempList.addAll(tempMap.get(expectedRelation));					
							tempList.add(queryId+" | "+direction+" | "+resultItem+" | "+provPoly);
							tempMap.put(expectedRelation, tempList);
							
						} else {
								
							String str = queryId+" | "+direction+" | "+resultItem+" | "+provPoly;
							ArrayList<String> tempList = new ArrayList();
							tempList.add(str);
							tempMap.put(expectedRelation, tempList);
						}
						
						cpToMetadataMap.put(cpVertexId, tempMap);
					} else {
							
						String str = queryId+" | "+direction+" | "+resultItem+" | "+provPoly;
						ArrayList<String> tempList = new ArrayList();
						tempList.add(str);
						HashMap<String, ArrayList<String>> tempMap = new HashMap();
						tempMap.put(expectedRelation, tempList);
						cpToMetadataMap.put(cpVertexId, tempMap);
					}
						
					if(cpVertexId.length() ==0) {
						System.out.println("VertexId empty!!\nVertexInfo: "+vertexInfo+"\tcp1Var: "+cp1Variable+"\t"+vertexInfoComp[1]);
						System.exit(0);
					}				
				} 

				/*
				 * Perform same steps for CP2
				 */
					
				if (!cp2Variable.equals("NULL") && resultVariable.contains(cp2Variable)) {
						
					String vertexInfoComp[] = vertexInfo.split(cp2Variable+"=");
					String temp = vertexInfoComp[1];
					temp = temp.substring(temp.indexOf("[")+1, temp.indexOf("]"));
					cpVertexId = temp;
					String tempDirection = new String();
						
					if (direction.equals("in"))
						tempDirection = "out";
					else
						tempDirection = "in";
	
						/*
						 * Add to the cp map	
						 */
						
					if (cpToMetadataMap.containsKey(cpVertexId)) {
							
						HashMap<String, ArrayList<String>> tempMap = new HashMap();
						tempMap.putAll(cpToMetadataMap.get(cpVertexId));
							
						if (tempMap.containsKey(expectedRelation)) {
							ArrayList<String> tempList = new ArrayList();
							tempList.addAll(tempMap.get(expectedRelation));
							tempList.add(queryId+" | "+tempDirection+" | "+resultItem+" | "+provPoly);						
							tempMap.put(expectedRelation, tempList);
						
						} else {
								
							String str = queryId+" | "+tempDirection+" | "+resultItem+" | "+provPoly;
							ArrayList<String> tempList = new ArrayList();
							tempList.add(str);
							 tempMap.put(expectedRelation, tempList);
						}

						cpToMetadataMap.put(cpVertexId, tempMap);
					} else {
	
						String str = queryId+" | "+tempDirection+" | "+resultItem+" | "+provPoly;
						ArrayList<String> tempList = new ArrayList();
						tempList.add(str);
						HashMap<String, ArrayList<String>> tempMap = new HashMap();
						tempMap.put(expectedRelation, tempList);
						cpToMetadataMap.put(cpVertexId, tempMap);
					}
						
					if(cpVertexId.length() ==0) {
						System.out.println("VertexId empty!!\nVertexInfo: "+vertexInfo+"\tcp1Var: "+cp2Variable+"\t"+vertexInfoComp[1]);
						System.exit(0);			
					}				
				} 
					
			} //Changing here end
		}
	}
	
	public static void buildEdgeCPMap(HashMap<String, HashMap<String, String>> subqueryResult) {
		
		HashMap<String, HashSet<String>> edgeToCPMap = new HashMap();
		edgeToCPMap.putAll(GlobalDSReader.getEdgeToCPMap());
		String resultFileBasePath = ConfigurationReader.get("SUBQUERY_RESULT");
		String cp1Variable = new String();
		String cp2Variable = new String();
		String resultVariable = new String();
		String expectedRelation = new String();
		String direction = new String();		
		HashMap<String, HashSet<String>> tempEdgeToCPMap = new HashMap();
		
		for (Entry<String, SubQuery> entry : subQueryMap.entrySet()) {
			
			String queryId = entry.getKey();
			SubQuery subquery = entry.getValue();
			cp1Variable = subquery.getCP1Variable();
			cp2Variable = subquery.getCP2Variable();
			resultVariable = subquery.getResultVariable();
			expectedRelation = subquery.getExpectedRelation();
			direction = subquery.getDirection();			
			HashMap<String, String> resultList = new HashMap();
			resultList.putAll(subqueryResult.get(queryId));

			
			String lineComp[];
			String result = new String();
			String vertexInfo = new String();
			String provPoly =new String();

			String resultItem = new String();
			String cpVertexId = new String();
				
			for (Entry<String, String> entry1: resultList.entrySet()) {
			
				lineComp = entry1.getKey().split("\t");
				result = lineComp[0];
				vertexInfo = lineComp[1];
				provPoly = entry1.getValue();
				String resultVarComp[] = resultVariable.split(", ");	
					
				if (resultVarComp.length == 1) {	
					resultItem = resultVarComp[0]+" = "+result;
					
				} else {
						resultItem = result;
				}
					
				if (!cp1Variable.equals("NULL") && resultVariable.contains(cp1Variable)) {
						
					String vertexInfoComp[] = vertexInfo.split(cp1Variable+"=");
					String temp = vertexInfoComp[1];	
					temp = temp.substring(temp.indexOf("[")+1, temp.indexOf("]"));
					cpVertexId = temp;
					
						/*
						 * Extract edge ids invovled in a polyProv and make entry
						 * corresponding to each edge
						 */
						
					HashSet<String> involvedEdgeCollection = new HashSet();	
					String polyComp[] = provPoly.split("\\+");

					for (String addend: polyComp) {
						
						String edgeId[] = addend.split("e");
						involvedEdgeCollection.addAll(Arrays.asList(edgeId));
					}
						
					for (String edgeId: involvedEdgeCollection) {
							
						if(edgeId.endsWith("."))
							edgeId = edgeId.substring(0, edgeId.length()-1);
							
						if (edgeToCPMap.containsKey(edgeId)) {
							edgeToCPMap.get(edgeId).add(cpVertexId+" : "+expectedRelation);
							tempEdgeToCPMap.put(edgeId, edgeToCPMap.get(edgeId));
						
						} else {
							HashSet<String> t = new HashSet();
							t.add(cpVertexId+" : "+expectedRelation);
							edgeToCPMap.put(edgeId, t);
							tempEdgeToCPMap.put(edgeId, edgeToCPMap.get(edgeId));
						}
					}						
				
				} else {
						System.out.print("Error: CP is empty!!");
						System.exit(0);
				}
					
					/*
					 * Perform same steps for CP2
					 */
					
				if (!cp2Variable.equals("NULL") && resultVariable.contains(cp2Variable)) {
						
					String vertexInfoComp[] = vertexInfo.split(cp2Variable+"=");
					String temp = vertexInfoComp[1];
					temp = temp.substring(temp.indexOf("[")+1, temp.indexOf("]"));
					cpVertexId = temp;
						
						/*
						 * Extract edge ids invovled in a polyProv and make entry
						 * corresponding to each edge
						 */
								
					HashSet<String> involvedEdgeCollection = new HashSet();	
					String polyComp[] = provPoly.split("\\+");
						
					for (String addend: polyComp) {
						
						String edgeId[] = addend.split("e");
						involvedEdgeCollection.addAll(Arrays.asList(edgeId));
					}
						
					for (String edgeId: involvedEdgeCollection) {
						
						if(edgeId.endsWith("."))
							edgeId = edgeId.substring(0, edgeId.length()-1);
							
					
						if (edgeToCPMap.containsKey(edgeId)) {
							edgeToCPMap.get(edgeId).add(cpVertexId+" : "+expectedRelation);
							tempEdgeToCPMap.put(edgeId, edgeToCPMap.get(edgeId));

						} else {
							HashSet<String> t = new HashSet();
							t.add(cpVertexId+" : "+expectedRelation);
							edgeToCPMap.put(edgeId, t);
							tempEdgeToCPMap.put(edgeId, edgeToCPMap.get(edgeId));
						}
					}
					
				} 
			}
		}
		
		GlobalDSReader.addToEdgeToCPMap(tempEdgeToCPMap);
		
	}

	
	public void annotateWithNeo () {
		
		String qStmt = "";

		HashMap<Long, HashMap<String, String>> tagCollection = new HashMap();
		
        for (Entry<String, HashMap<String, ArrayList<String>>> entry: cpToMetadataMap.entrySet()) {
        	
			String vid = entry.getKey();
			Long vID = Long.parseLong(vid); // TODO: Lookup for the Id of vertex labelled: entry.getKey()

        	HashMap<String, String> tagRelationMap = new HashMap();
    		
    		tagRelationMap.putAll(readTag(vID));
    		
    //		if (tagRelationMap.size() > 0) {
    			
    			String relVal = "";
    			String newTag = "";
    			String oldTag = "";
    			
    			for (Entry<String, ArrayList<String>> e: entry.getValue().entrySet()) {
    				
    				relVal = e.getKey();
    				
    				if (tagRelationMap.containsKey(relVal)) {
    					
    					for (String tag: e.getValue()) {
    						
        					oldTag = tagRelationMap.get(relVal);	
    						String tagComp[] = tag.split(" \\| ");
    						String toSearch =  tagComp[0]+" | "+tagComp[1]+" | "+tagComp[2]+" | ";
    						
    						if (oldTag.contains(toSearch)) {
    							
    							String temp1 = toSearch.replaceAll("[\\<\\(\\[\\{\\\\\\^\\-\\=\\$\\!\\|\\]\\}\\)\\?\\*\\+\\.\\>]", "\\\\$0"); 
    							String oldTagComp[] = oldTag.split(temp1);
    							String oldPoly = "";
    							String newPoly = "";
    							
    							if (oldTagComp[1].contains(";")) {
    								oldPoly = oldTagComp[1].substring(0, oldTagComp[1].indexOf(";"));
    							
    							} else {
    								oldPoly = oldTagComp[1];
    							}
    							
    							newPoly = oldPoly.concat("+").concat(tagComp[3]);
    							String oldList = toSearch+oldPoly;
    							String newList = toSearch+newPoly;
    							newTag = oldTag.replace(oldList, newList);
    							
    						} else {
    							newTag = oldTag.concat("; "+tag);
    						}
    						
							tagRelationMap.put(relVal, newTag);
    					}
    				} else {
    					String tag = "";
    		   			for (String str: e.getValue()) {
    	    				
    	    				tag = tag.concat(str+"; ");
    	    			}
    	    			
    	    			tag = tag.substring(0, tag.lastIndexOf(";"));
						tagRelationMap.put(relVal, tag);
    				}
    			}

    			tagCollection.put(vID, tagRelationMap);
    		}
		
        updateTag(tagCollection);
	}


	private Map<? extends String, ? extends String> readTag(Long vID) {

		HashMap<String, String> relationMap = new HashMap();
		/*
		 * Get vertex and get its "tag" property
		 */
		String tag = "";

		try(Transaction tx = db.beginTx()) {
		
			Node node = db.getNodeById(vID);
			
			if(node.hasProperty("tag")) {
				tag = (String) node.getProperty("tag");
			
			} else {
				return relationMap;
			}
			
			tx.success();
		}
		
		String tagComp[] = tag.split(" RELATION__");
		
		for(String str: tagComp) {

			if(str.length()>0) {
				String strComp[] = str.split("->");
				relationMap.put(strComp[0],strComp[1]);
			}
		}
		
		return relationMap;
	}


	private void updateTag(Long vID, HashMap<String, String> tagRelationMap) {
		
		HashMap<String, String> relationMap = new HashMap();
		/*
		 * Get vertex and get its "tag" property
		 */
		String tag = "";

		for(String relation : tagRelationMap.keySet()) {
			
			tag = tag.concat(" RELATION__"+relation+"->"+tagRelationMap.get(relation));
		}
		
		try(Transaction tx = db.beginTx()) {
			Node node = db.getNodeById(vID);
			node.setProperty("tag",tag);
			tx.success();
		}
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
		}	
	}

}
