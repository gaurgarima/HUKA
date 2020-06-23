package maintenance.insertion;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import preprocessing.queryRegistry.annotator.*;
import preprocessing.queryRegistry.executionPlan.global.InternalRelation;

import main.ConfigurationReader;
import main.GlobalDSReader;

public class InsertionHandler {

	/**
	 * @param args
	 */
	static HashMap<String, ArrayList<String>> resultDeltaCollection;
	static HashMap<String, SubQuery> subQueryMap;
	static String dataset;
	static HashSet<String> outQuerySet;
	static HashSet<String> inQuerySet;
	static HashMap<String, ArrayList<String>> outQueryToDeltaResultMap ;
	static HashMap<String, ArrayList<String>> inQueryToDeltaResultMap ;

	
	static 	GraphDatabaseService db;

	public InsertionHandler (String dataset) throws IOException, ClassNotFoundException {

		this.dataset = dataset;
		subQueryMap = new HashMap();
		subQueryMap.putAll(GlobalDSReader.getSubQueryMap());
		db = GlobalDSReader.getDBInstance();
	}
	
	
	private static void showResult(HashMap<String, ArrayList<String>> resultDeltaCollection2) throws IOException {

		HashMap<String, ArrayList<String>> queryToChangeMap = new HashMap();
		
		for(Entry<String, ArrayList<String>> entry : resultDeltaCollection2.entrySet()) {
			
			String queryId = entry.getKey();
			ArrayList<String> deltaResult = new ArrayList();

			for (String delta : entry.getValue()) {

				String deltaComp[] = delta.split(":");
				deltaResult.addAll(computeDeltaResult(delta));
			}
			
			queryToChangeMap.put(queryId, deltaResult);
		}
		
		updateQueryResult(queryToChangeMap);		
	}


	private static Collection<? extends String> computeDeltaResult(String delta) {

		String deltaComp[] = delta.split(" : ");
		String deltaType = new String();
		deltaType = deltaComp[0];
		String outPart = deltaComp[1];
		String inPart = deltaComp[2];
		ArrayList<String> deltaResult = new ArrayList();
				
		if (deltaType.equals("ISOLATED") || deltaType.equals("CONNECTED")) {
			
			String answers[] = outPart.split("; ");
			
			for (int i = 0; i < answers.length ; i++) {
								
				String temp[] = answers[i].split(" \\| ");
				String poly = temp[1];
				String finalPoly = poly;
				
				if (poly.contains("+")) {
					String s[] = poly.split("\\+"); 
					finalPoly = "";
					
					for (int j = 0; j< s.length; j++) {
						finalPoly = finalPoly.concat(s[j].concat(inPart+"+"));
					}
					
					finalPoly = finalPoly.substring(0, finalPoly.lastIndexOf("+"));
				} else {
					
					finalPoly = finalPoly.concat(inPart);
				}
				
				deltaResult.add(temp[0]+"\t"+finalPoly);
			}
			
		} else if (deltaType.equals("COUNTER")) {
			
			String outResult[] = outPart.split("; ");
		
			String inResult[] = inPart.split("; ");
			String outResultItem, outPoly,inResultItem, inPoly;
			String newEdgeId = deltaComp[3];
			
			for (int i =0; i < outResult.length; i++) {
				
				outResultItem = outResult[i].split(" \\| ")[0];
				outPoly = outResult[i].split(" \\| ")[1];

				ArrayList<String> outPolyTerms = new ArrayList(Arrays.asList(outPoly.split("\\+")));
				
				for (int j = 0; j < inResult.length; j++) {
					
					inResultItem = inResult[i].split(" \\| ")[0];
					inPoly = inResult[i].split(" \\| ")[1];
					
					ArrayList<String> inPolyTerms = new ArrayList(Arrays.asList(inPoly.split("\\+")));
					String deltaPoly = new String();
					
					for (String outTerm : outPolyTerms) {
						for (String inTerm : inPolyTerms)
							deltaPoly = deltaPoly.concat(outTerm+inTerm+newEdgeId+"+");
					}
					
					deltaPoly = deltaPoly.substring(0,deltaPoly.lastIndexOf("+"));
					
					deltaResult.add(outResultItem+", "+inResultItem+"\t"+deltaPoly);
				}
			}
			
		} else if (deltaType.equals("TDB")) {
			
			String answers[] = outPart.split("; ");
			String inPartComp[]  =inPart.split(" \\. ");
			
			for (int i = 0; i < answers.length ; i++) {
								
				String temp[] = answers[i].split(" \\| ");
				String poly = temp[1];
				String finalPoly = poly;
				
				if (poly.contains("+")) {
					
					String s[] = poly.split("\\+"); 
					finalPoly = "";
					
					for (int j = 0; j< s.length; j++) {
			
						for (int k = 1; k < inPartComp.length; k++) {
							
							finalPoly = finalPoly.concat(s[j].concat(inPartComp[k]+inPartComp[0]+"+"));
						}
					}
					
				} else {

					for (int k = 0; k < inPartComp.length; k++) {						
						finalPoly = finalPoly.concat(poly.concat(inPartComp[k]+inPartComp[0]+"+"));
					}
				}
				
				finalPoly = finalPoly.substring(0, finalPoly.lastIndexOf("+"));
				deltaResult.add(temp[0]+"\t"+finalPoly);
			}

		} else {
	
			System.out.print("Error: Not a valid deltaType!!");
			System.exit(0);	
		}
		
		return deltaResult;
	}


	private static void updateQueryResult(HashMap<String, ArrayList<String>> queryToChangeMap) throws IOException {
		
		/*
		 * Read the old result of the main queries
		 */
		boolean flag = true;
		
		for (Entry<String, ArrayList<String>> entry: queryToChangeMap.entrySet()) {
			
			String qId = entry.getKey();				
			String resultFile = ConfigurationReader.get("QUERY_RESULT_FILE");
			BufferedReader br = new BufferedReader(new FileReader(new File(resultFile+"Q_"+qId+".txt")));	//Change path
			HashMap<HashSet<String>, String> oldResultSet = new HashMap();
			String line;
			
			while ((line = br.readLine()) != null) {
				
				String lineComp[] = line.split("\t");
				HashSet temp = new HashSet<String>();
				temp.addAll(Arrays.asList(lineComp[0].split(", ")));
				oldResultSet.put(temp, lineComp[1]);
			}
			
			
			br.close();
			HashMap<HashSet<String>, String> newResultSet = new HashMap();
			newResultSet.putAll(oldResultSet);		
			
			for (String item : entry.getValue()) {
				
				String itemComp[] = item.split("\t");
				String resultItem = itemComp[0];
				HashSet<String> temp = new HashSet();
				temp.addAll(Arrays.asList(resultItem.split(", ")));
				String poly = itemComp[1];
							
				if (newResultSet.containsKey(temp)) {
					
					String oldPoly = newResultSet.get(temp);
					String newPoly = oldPoly.concat("+").concat(poly);
					newResultSet.put(temp,newPoly);
				
				} else {
					newResultSet.put(temp, poly);
				}
			}
			
			BufferedWriter bw = new BufferedWriter(new FileWriter( new File(resultFile+"Q_"+qId+".txt"))); 
			String item = "";
			
			for (Entry<HashSet<String>, String> entry1: newResultSet.entrySet()) {	
				
				for (String s: entry1.getKey()) {
					item = item.concat(s+", ");
				}

				item = item.substring(0, item.lastIndexOf(","));
				bw.write(item+"\t"+entry1.getValue()+"\n");
				item = "";
			}
			
			bw.close();
		}
	}

	

	
		public String insertionWithSpecialQuery (String subject, String object, String relation, String newEdgeDefId, 
			Long outVertexId, Long inVertexId) throws NumberFormatException, SQLException, IOException, ClassNotFoundException {
		
		int affectedQueryCount = 0;
		
		/*
		 * Main change to accommodate the 1:m potential matches
		 */
		Long st = System.nanoTime();	
		SubQueryUpdation updater = new SubQueryUpdation(dataset);
		Long timeTakenToUpdate = updater.insert(subject, object, relation, newEdgeDefId);
		HashMap<String, ArrayList<String>> deltaAdditionalResults = new HashMap();
		
		deltaAdditionalResults.putAll(getAdditionalAnswers(relation,updater));
		affectedQueryCount = deltaAdditionalResults.size();
		
		HashMap<String, ArrayList<String>> resultDeltaCollection = new HashMap<String, ArrayList<String>>();
		Long affectedParentSt = System.nanoTime();
		resultDeltaCollection.putAll(insertWithNeo(outVertexId, inVertexId, relation, newEdgeDefId));

		
		Long affectedParentEt = System.nanoTime();
		affectedQueryCount = affectedQueryCount + resultDeltaCollection.size();

		
		/*
		 * Update the query result
		 */
		Long updateParentEt = System.nanoTime();
		Long updateParentSt = (long) 0;
		long findUpdateEt = 0;
		Long invertIndexUpdateSt = (long) 0;
		Long invertIndexUpdateEt = (long) 0;
	
		if (affectedQueryCount > 0) {
			
			updateParentSt = System.nanoTime();
			HashMap<String, ArrayList<String>> queryToChangeMap = new HashMap();
			boolean flag = true;

			for(Entry<String, ArrayList<String>> entry : resultDeltaCollection.entrySet()) {
				
				String queryId = entry.getKey();
				ArrayList<String> deltaResult = new ArrayList();

				for (String delta : entry.getValue()) {					
					deltaResult.addAll(computeDeltaResult(delta));
				}
				
				queryToChangeMap.put(queryId, deltaResult);

			}
			
			// additional delta results			
			
			for (Entry<String, ArrayList<String>> entry2: deltaAdditionalResults.entrySet()) {
				
				if (queryToChangeMap.containsKey(entry2.getKey()))
					queryToChangeMap.get(entry2.getKey()).addAll(entry2.getValue());
				else
					queryToChangeMap.put(entry2.getKey(), entry2.getValue());	
			}		
			
			findUpdateEt = System.nanoTime();
			updateQueryResult(queryToChangeMap);
			updateParentEt = System.nanoTime();
			invertIndexUpdateSt = System.nanoTime();

			HashMap<String,HashMap<String,ArrayList<String>>> invertedIndex = new HashMap();
			HashMap<String,HashMap<String,ArrayList<String>>> deltaInvertedIndex = new HashMap(); 
			invertedIndex.putAll(GlobalDSReader.getInvertedIndex());
			
			for (Entry<String, ArrayList<String>> entry: queryToChangeMap.entrySet()) {
				
				String queryId = entry.getKey();
				
				for (String str: entry.getValue()) {
					
					String strComp[] = str.split("\t");
					String resultItem = strComp[0];
					String poly = strComp[1];
					HashSet<String> edgeSet = new HashSet();
						
					if (poly.contains("+")) {
						String addends[] = poly.split("\\+");
						
						for (String addend : addends) {
							String edges[] = addend.split("e");
							
							edgeSet.addAll(Arrays.asList(edges));
						}
					} else {
						String edges[] = poly.split("e");
						edgeSet.addAll(Arrays.asList(edges));
					}
					
					/*
					 * Change the invertedIndex
					 */
					
					for (String edgeId: edgeSet) {
						
						String eId = "e"+edgeId;
						
						if(eId.endsWith("."))
							eId = eId.substring(0, eId.length()-1);
						
						if (invertedIndex.containsKey(eId)) {
							HashMap<String, ArrayList<String>> temp = new HashMap();
							temp.putAll(invertedIndex.get(eId));
							
							if (temp.containsKey(queryId)) {
								temp.get(queryId).add(resultItem);
							} else {
								ArrayList<String> l = new ArrayList();
								l.add(resultItem);
								temp.put(queryId, l);
							}
							invertedIndex.put(eId, temp);
							deltaInvertedIndex.put(eId, temp);
						
						} else {
							HashMap<String, ArrayList<String>> temp = new HashMap();
							ArrayList<String> l = new ArrayList();
							l.add(resultItem);
							temp.put(queryId, l);
							invertedIndex.put(eId, temp);

							deltaInvertedIndex.put(eId, temp);
						}
					}
				}
				
			}
			
			GlobalDSReader.addToInvertedIndex(deltaInvertedIndex);
			 
			invertIndexUpdateEt = System.nanoTime();
		}

		Long et = System.nanoTime();
		long txSt = System.nanoTime();

		long txEt = System.nanoTime();

		Long subqueryUpdateSt = System.nanoTime();

		String updateTime=	updater.updateDS(); // change
	
		Long subqueryUpdateEt = System.nanoTime();
		
		Long overallTimeTaken = et-st;
		Long findAffectedParent = affectedParentEt - affectedParentSt;
		Long updateParent = updateParentEt - updateParentSt;
		Long invertIndexUpdate = invertIndexUpdateEt -invertIndexUpdateSt;
		long updateFind = findUpdateEt - updateParentSt;
		Long subQueryUpdate = subqueryUpdateEt - subqueryUpdateSt;

		return Long.toString(updateParentEt-st)+"\t"+Long.toString(invertIndexUpdateEt-invertIndexUpdateSt)+"\t"+updateTime;
	}
	
	
	
	public String insertion (String subject, String object, String relation, String newEdgeDefId, 
			Long outVertexId, Long inVertexId) throws NumberFormatException, SQLException, IOException, ClassNotFoundException {
		
		int affectedQueryCount = 0;
		HashMap<String, ArrayList<String>> resultDeltaCollection = new HashMap<String, ArrayList<String>>();
		Long st = System.nanoTime();

		Long affectedParentSt = System.nanoTime();
		resultDeltaCollection.putAll(insertWithNeo(outVertexId, inVertexId, relation, newEdgeDefId));
		Long affectedParentEt = System.nanoTime();
		
		affectedQueryCount = affectedQueryCount + resultDeltaCollection.size();

		/*
		 * Update the query result
		 */
		Long updateParentEt = System.nanoTime();
		Long updateParentSt = (long) 0;
		long findUpdateEt = 0;
		Long invertIndexUpdateSt = (long) 0;
		Long invertIndexUpdateEt = (long) 0;
		
		if (affectedQueryCount > 0) {
			
			updateParentSt = System.nanoTime();
			HashMap<String, ArrayList<String>> queryToChangeMap = new HashMap();
			boolean flag = true;

			for(Entry<String, ArrayList<String>> entry : resultDeltaCollection.entrySet()) {
				
				String queryId = entry.getKey();
				ArrayList<String> deltaResult = new ArrayList();

				for (String delta : entry.getValue()) {					
					deltaResult.addAll(computeDeltaResult(delta));
				}
				
				queryToChangeMap.put(queryId, deltaResult);
			}
			
			findUpdateEt = System.nanoTime();
			updateQueryResult(queryToChangeMap);
			updateParentEt = System.nanoTime();
			
			invertIndexUpdateSt = System.nanoTime();
			HashMap<String,HashMap<String,ArrayList<String>>> invertedIndex = new HashMap();
			HashMap<String,HashMap<String,ArrayList<String>>> deltaInvertedIndex = new HashMap();
			 
			invertedIndex.putAll(GlobalDSReader.getInvertedIndex());
			
			for (Entry<String, ArrayList<String>> entry: queryToChangeMap.entrySet()) {
				
				String queryId = entry.getKey();
				
				for (String str: entry.getValue()) {
					
					String strComp[] = str.split("\t");
					String resultItem = strComp[0];
					String poly = strComp[1];
					HashSet<String> edgeSet = new HashSet();
						
					if (poly.contains("+")) {
						String addends[] = poly.split("\\+");
						
						for (String addend : addends) {
							String edges[] = addend.split("e");
							
							edgeSet.addAll(Arrays.asList(edges));
						}
					} else {
						String edges[] = poly.split("e");
						edgeSet.addAll(Arrays.asList(edges));
					}
					
					/*
					 * Change the invertedIndex
					 */
					
					for (String edgeId: edgeSet) {
						
						String eId = "e"+edgeId;
						
						if(eId.endsWith("."))
							eId = eId.substring(0, eId.length()-1);
						
						if (invertedIndex.containsKey(eId)) {
							HashMap<String, ArrayList<String>> temp = new HashMap();
							temp.putAll(invertedIndex.get(eId));
							
							if (temp.containsKey(queryId)) {
								temp.get(queryId).add(resultItem);
							} else {
								ArrayList<String> l = new ArrayList();
								l.add(resultItem);
								temp.put(queryId, l);
							}
			
							invertedIndex.put(eId, temp);
							deltaInvertedIndex.put(eId, temp);
						
						} else {
							HashMap<String, ArrayList<String>> temp = new HashMap();
							ArrayList<String> l = new ArrayList();
							l.add(resultItem);
							temp.put(queryId, l);
							invertedIndex.put(eId, temp);
							deltaInvertedIndex.put(eId, temp);
						}
					}
				}
				
			}
			
			GlobalDSReader.addToInvertedIndex(deltaInvertedIndex); 
			invertIndexUpdateEt = System.nanoTime();
		}

		Long et = System.nanoTime();

		Long subqueryUpdateSt = System.nanoTime();
		String subqueryTime = updateSubquery(subject, object, relation,newEdgeDefId);
		Long subqueryUpdateEt = System.nanoTime();
		
		Long overallTimeTaken = et-st;
		Long findAffectedParent = affectedParentEt - affectedParentSt;
		Long updateParent = updateParentEt - updateParentSt;
		Long invertIndexUpdate = invertIndexUpdateEt -invertIndexUpdateSt;
		long updateFind = findUpdateEt - updateParentSt;
		Long subQueryUpdate = subqueryUpdateEt - subqueryUpdateSt;

	/*	System.out.println("\nOverall insertion summary\n");

		System.out.println("\nInsertionHandler:Insertion:overall:\t"+Long.toString(overallTimeTaken));
		System.out.println("\nInsertionHandler:Insertion:findAffectedParentQuery:\t"+Long.toString(findAffectedParent));
		System.out.println("\nInsertionHandler:Insertion:findParentUpdate:\t"+Long.toString(updateFind));
		System.out.println("\nInsertionHandler:Insertion:UpdateParent:\t"+Long.toString(updateParent));
		System.out.println("\nInsertionHandler:Insertion:invretedIndexUpdate:\t"+Long.toString(invertIndexUpdate));
		System.out.println("\nInsertionHandler:Insertion:subQueryUpdateOverall:\t"+Long.toString(subQueryUpdate));

		*/
		return Long.toString(updateParentEt-st)+"\t"+Long.toString(invertIndexUpdateEt-invertIndexUpdateSt)+"\t"+subqueryTime;

	}


	private String updateSubquery(String subject, String object, String relation, String edgeId) throws IOException, SQLException, ClassNotFoundException {
		
		SubQueryUpdation updater = new SubQueryUpdation(dataset);
		Long timeTakenToUpdate = updater.insert(subject, object, relation, edgeId);
		String updateTime = updater.updateDS(); 

		return Long.toString(timeTakenToUpdate)+"\t"+updateTime;
		
	}
	
	private HashMap<String, ArrayList<String>> getAdditionalAnswers(String predicate, SubQueryUpdation updater ) throws SQLException {
		
		HashMap<String, String> subqueryToInterNode = new HashMap();
		HashMap<String,String> queryToSubqueryMap = new HashMap();
		HashMap<String, HashMap<String, ArrayList<String>>> queryToSameEdge = new HashMap();
		HashMap<String, ArrayList<String>> deltaResult = new HashMap();
		
		HashMap<String, InternalRelation> internalRelationMap = new HashMap();
		internalRelationMap.putAll(GlobalDSReader.getInternalRelationMap());
		
		Statement statement = GlobalDSReader.getSQLStatement();
	
		queryToSubqueryMap.putAll(GlobalDSReader.getpredicateToAffectedQueryMap().get(predicate));
		subqueryToInterNode.putAll(GlobalDSReader.getSubqueryToInternalNodeMap());
		queryToSameEdge.putAll(GlobalDSReader.getQueryToSameEdge());
		
		for(String queryId: queryToSubqueryMap.keySet()) {
			
			String subqueryId = queryToSubqueryMap.get(queryId);
			String internalNodeId = subqueryToInterNode.get(subqueryId);
			
			if (updater.getPropagator().getComputedResults().contains(internalNodeId)) {
				
				HashMap<String, String> qVarToAttributeMap = new HashMap();
				ArrayList<String> newResultItems = new ArrayList();
				qVarToAttributeMap.putAll(internalRelationMap.get(internalNodeId).getQVarMap().get(queryId));
				
				String qStmt = "SELECT * FROM delta_"+internalNodeId+";";
		        ResultSet rs = statement.executeQuery(qStmt);
		        
		        String var1 = new String();
		        String var2 = new String();
		        String attr1 = new String();
		        String attr2 = new String();
		        
		        ArrayList<ArrayList<String>> attr = new ArrayList();
		        
		        for (String group: queryToSameEdge.get(queryId).get(predicate)) {
		        	
		        	String groupComp[] = group.split(" : ");
		        	ArrayList<String> compList = new ArrayList();

		        	for (int i = 0; i < 2; i ++) {
		        		
		        		String sComp[] = groupComp[i].split(", ");
			        	
			        	String attrList = "";
			        	
		        		for (String attribute: sComp) {
		        			
		        			if (attribute.startsWith("?")) {
		        				if (qVarToAttributeMap.containsKey(attribute))		        					
		        					attrList = attrList.concat(qVarToAttributeMap.get(attribute)+", ");
			        		} else {
			        			attrList = attrList.concat(attribute+", ");
			        		}
		        		}
		        		
		        		attrList = attrList.substring(0, attrList.lastIndexOf(", "));
		        		compList.add(i, attrList);
		        	}
		        	attr.add(compList);

		        }
		                
		        ArrayList<String> toProject = new ArrayList();
				SubQuery subquery = subQueryMap.get(subqueryId);
        		String missingRelation = subquery.getExpectedRelation();
        		String cp1Variable = subquery.getCP1Variable();
        		String cp2Variable = subquery.getCP2Variable();
        		String cp2Condition = subquery.getCP2Condition();
    			String direction = subquery.getDirection();
    			String resultVar = subquery.getResultVariable();
    			
    			for (String str: resultVar.split(", ")) {
    				toProject.add(str);
    			}
    			
		        while (rs.next()) {
		        	
	        		boolean isTrue = true;

			        String pmPoly = new String();
			        pmPoly = rs.getString("poly");
			        
		        	for (ArrayList<String> nodeList: attr) {
			        	
		        		isTrue = true;
		        		
		        		for (int j = 0 ; j < 2; j++) {
		        		
		        			String attrComp[] = nodeList.get(j).split(", ");
		        			
		        			for (int i = 0; i < attrComp.length; i++) {
		        				
		        				if (attrComp[i].startsWith("?"))
		        					attr1 = rs.getString(attrComp[i]);
		        				else
		        					attr1 = attrComp[i];
		        				
		        				for (int k = i +1; k < attrComp.length; k++) {
		        					
		        					if (attrComp[k].startsWith("?"))
		        						attr2 = rs.getString(attrComp[k]);
		        					else
		        						attr2 = attrComp[k];
		        					
		        					if (!attr1.equals(attr2)) {
		        						isTrue = false;
		        						break;
		        					}
		        				}
		        				
		        				if (!isTrue) {
		        					break;
		        				}
		        			}
		        			
		        			if (!isTrue) {
		        				break;
		        			}
		        
		        		}
		        		
		        		if (isTrue) {
		        			break;
		        		}
		        	}
			        
		        	
		        	if (isTrue) { // i.e. common edges matches; next check the missing part of this subquery
		        		
		           		String outV = new String();
	        			String inV = new String();

		        		if ((!cp1Variable.equals("NULL")) && (!cp2Variable.equals("NULL"))) { // connected component subquery
		        			
		        			String outVAttr = qVarToAttributeMap.get("?"+cp1Variable);
		        			String inVAttr = qVarToAttributeMap.get("?"+cp2Variable);
		        			
		        			outV = rs.getString(outVAttr);
		        			inV = rs.getString(inVAttr);
		        			
		        			qStmt = "Select * from "+missingRelation+" where `out` = "+outV+" and `in` = "+inV+";";
		        			
		        		} else { // isolated type of subquery
		        				
		        			String condComp[] = cp2Condition.split(" : ");
		        			String outAttr = new String();
		        			String inAttr = new String();
		        			
		        			if (direction.equals("out")) {
		        				outAttr = qVarToAttributeMap.get("?"+cp1Variable);
		        				inV = condComp[1].trim();
		        				outV = rs.getString(outAttr);
		        				
		        			} else {
		        				inAttr = qVarToAttributeMap.get("?"+cp1Variable);
		        				inV = rs.getString(inAttr);
		        				outV = condComp[1].trim();
		        			}
		        			
		        			if (!cp2Condition.equals("NULL")) {
		        			
		        				qStmt = "Select * from "+missingRelation+" where `out` = "+outV+" and `in` = "+inV+";";
			        					
		        			} else {
		        			
		        					if (direction.equals("out"))
		        						qStmt = "Select * from "+missingRelation+" where `out` = "+outV+";";
		        					else
		        						qStmt = "Select * from "+missingRelation+" where `in` = "+inV+";";
		        					
		        			}
			        			
		        		} // end of isolated subquery condition
		        			
		        				
		        		
		        		ResultSet rs1 = statement.executeQuery(qStmt);
	        			
	        			 while (rs1.next()) {

	        				String edgePoly = rs1.getString("poly");
	        				String newPoly = pmPoly.concat("."+edgePoly);
	        				String newItem = "";
	        				
	        				for (String var: toProject) {
	        				
	        		
	        					String item = qVarToAttributeMap.get("?"+var);
	        					String val = "";
	        					
	        					try {
	        						rs.findColumn(item);
	        						val = rs.getString(item);
	        						
	        					} catch (Exception e) {
	        					
	        						if (cp2Variable.equals("NULL")) {
	        							
	        							if (direction.equals("out"))
	        								val = rs1.getString("in");
	        							else
	        								val=  rs1.getString("out");
	        						}
	        					}
	        					
	        					newItem = newItem.concat(var+" = ").concat(val+", ");
	        				}
	        				
	        				newItem = newItem.substring(0, newItem.lastIndexOf(","));
	        				
	        				newResultItems.add(newItem+"\t"+newPoly);
	        			}
	        			rs1.close();
	        	
		        	} // got new answers by checking the complete pattern
		        			
		        } // resultset of one query ends here
		        
		        
		      
		        if (newResultItems.size() >0) {
		        	deltaResult.put(queryId, newResultItems);
		        }
		        
				rs.close();
				
			} // frst condition to check if the sbugraph has any answer
			
		} // check for all the relevant queries ends
		
		return deltaResult;
		
	}
	
	
	private static Map<? extends String, ? extends ArrayList<String>> insertWithNeo(		
			Long outVertexId, Long inVertexId, String relation,
			String newEdgeDefId) throws NumberFormatException, SQLException {
		
		
		
		 outQuerySet = new HashSet();
		 inQuerySet = new HashSet();
		 outQueryToDeltaResultMap = new HashMap();
		 inQueryToDeltaResultMap = new HashMap();

		 HashSet<String> affectedQuerySet = new HashSet();
		HashMap<String, ArrayList<String>> deltaResultMap = new HashMap();

		HashMap<String, String> outTagMap = new HashMap();
		outTagMap.putAll(readTag(outVertexId));
			
		String outV = outTagMap.get("uri");
			
		if (outTagMap.containsKey(relation)) {
			
			String qlist = outTagMap.get(relation);
			String[] qlistComp = qlist.split("; ");
				
			for(String qid: qlistComp)
			{
				if (qid.length()>0) {
						
					String qidComp[] = qid.split(" \\| ");
					String id = qidComp[0];
					String dir = qidComp[1];
										
					if(dir.equals("out")) {
						outQuerySet.add(id);
						
						if (outQueryToDeltaResultMap.containsKey(id)) {
							outQueryToDeltaResultMap.get(id).add(qidComp[2]+" | "+qidComp[3]);
						} else {
							ArrayList<String> temp = new ArrayList();
							temp.add(qidComp[2]+" | "+qidComp[3]);
							outQueryToDeltaResultMap.put(id, temp);
						}
					}
				  }	
				}
				
			}
			
			
		boolean emptyProperty = false;
			
			
		HashMap<String, String> inTagMap = new HashMap();
		inTagMap.putAll(readTag(inVertexId));
		String inV = inTagMap.get("uri");
			
		if (inTagMap.containsKey(relation)) {

			String qlist = inTagMap.get(relation);				
			String[] qlist_comp = qlist.split("; ");	
				
			for(String qid: qlist_comp) {

				if (qid.length()>0) {
					String qidComp[] = qid.split(" \\| ");
					String id = qidComp[0];
					String dir = qidComp[1];
					
					if(dir.equals("in")) {
						inQuerySet.add(id);
						
						if (inQueryToDeltaResultMap.containsKey(id)) {
							inQueryToDeltaResultMap.get(id).add(qidComp[2]+" | "+qidComp[3]);
						} else {
							ArrayList<String> temp = new ArrayList();
							temp.add(qidComp[2]+" | "+qidComp[3]);
							inQueryToDeltaResultMap.put(id, temp);
						}
					}
				}	
			}			
		}

			

		// ******************** Handle Out_set of  CPs ****************

		boolean flag1 = true;
			
		for(String qid : outQuerySet){
				
			SubQuery sQuery = subQueryMap.get(qid);
			String counterQid = sQuery.getCounterQuery();
			String cp2Identifier = sQuery.getCP2Variable();
				
				
			if (counterQid.startsWith("G _ ")) {				// Type 3
					
				String counterComp[] = counterQid.split(" _ ");
				String direction = counterComp[2];
				String tdbName = counterComp[1];
				String tdbEdgeId;
				tdbEdgeId = sQuery.checkCP2Condition(inV, tdbName, direction);
					
	
				if (tdbEdgeId.length() > 1) {	

					String parentId = sQuery.getParentQueryId();
					affectedQuerySet.add(parentId);

						/*
						 * Tracking the delta result which needs to be added in the final result
						 */
						
					ArrayList<String> temp = new ArrayList();
					temp.addAll(outQueryToDeltaResultMap.get(qid));
					String delta = new String();
					// TDB:resultItem1|propoly1; resultItem2|provPoly2:newEdgeDefId.TDBEdgeId
					delta = "TDB : ";
						
					for (String s : temp) {
						delta = delta.concat(s+"; ");
					}
						
					delta = delta.substring(0, delta.lastIndexOf(";"));
					delta = delta.concat(" : ").concat("e"+newEdgeDefId).concat(" . "+tdbEdgeId);
						
					if (deltaResultMap.containsKey(parentId)) {
						deltaResultMap.get(parentId).add(delta);

					} else {
						ArrayList<String> t = new ArrayList();
						t.add(delta);
						deltaResultMap.put(parentId, t);
					}
				}		
			} else{
					
				if(counterQid.startsWith("Q : ")){			//Type 2
						
					String counterComp[]= counterQid.split(" : ");
					String counterQname= counterComp[1];
			
					if(inQuerySet.contains(counterQname)){
							
						inQuerySet.remove(counterQname);
						String parentId = sQuery.getParentQueryId();						
						affectedQuerySet.add(parentId);

						/*
						 * COUNTER:resultItem1|provPoly1, resultItem2|ProvPoly2:resultItem4|provPoly4, resultItem5|provPoly5
						 */
						ArrayList<String> tempOut = new ArrayList();
						ArrayList<String> tempIn = new ArrayList();
						String delta = "COUNTER : ";
							
						tempOut.addAll(outQueryToDeltaResultMap.get(qid));
						tempIn.addAll(inQueryToDeltaResultMap.get(counterQname));
							
						for (String s : tempOut) {
							delta = delta.concat(s+"; ");
						}
							
						delta = delta.substring(0, delta.lastIndexOf(";"));
						delta = delta.concat(" : ");
							
						for (String s : tempIn) {
							delta = delta.concat(s+"; ");
						}
						
						delta = delta.substring(0, delta.lastIndexOf(";"));
						delta = delta.concat(" : e"+newEdgeDefId);
							
						if (deltaResultMap.containsKey(parentId)) {
							deltaResultMap.get(parentId).add(delta);
						} else {

							ArrayList<String> t = new ArrayList();
							t.add(delta);
							deltaResultMap.put(parentId, t);
						}
					}		
				} else{
						
					if(cp2Identifier.equals("NULL")){			// Type 0
							
						if(sQuery.checkCP2Condition(inV)){
								
							String parentId = sQuery.getParentQueryId();
							affectedQuerySet.add(parentId);
							ArrayList<String> temp = new ArrayList();
							temp.addAll(outQueryToDeltaResultMap.get(qid));
							String delta = new String();
		
							// ISOLATED:resultItem1|propoly1, resultItem2|provPoly2:newEdgeDefId
							delta = "ISOLATED : ";
								
							for (String s : temp) {
								delta = delta.concat(s+"; ");
							}
								
							delta = delta.substring(0, delta.lastIndexOf(";"));
							delta = delta.concat(" : ").concat("e"+newEdgeDefId);
								
							if (deltaResultMap.containsKey(parentId)) {
								deltaResultMap.get(parentId).add(delta);

							} else {
								ArrayList<String> t = new ArrayList();
								t.add(delta);
								deltaResultMap.put(parentId, t);
							}
							
						}
							
					} else{														// Type 1
							
					
						if(sQuery.checkCPCondition(outV) && sQuery.checkCP2Condition(inV)){

							String parentId = sQuery.getParentQueryId();
							affectedQuerySet.add(parentId);
							inQuerySet.remove(qid);
							ArrayList<String> temp = new ArrayList();
							temp.addAll(outQueryToDeltaResultMap.get(qid));
							String delta = new String();

							//CONNECTED:resultItem1|propoly1, resultItem2|provPoly2:newEdgeDefId
							delta = "CONNECTED : ";
								
							for (String s : temp) {
								delta = delta.concat(s+"; ");
							}
								
							delta = delta.substring(0, delta.lastIndexOf(";"));
							delta = delta.concat(" : ").concat("e"+newEdgeDefId);
								
							if (deltaResultMap.containsKey(parentId)) {
								deltaResultMap.get(parentId).add(delta);
						
							} else {
								ArrayList<String> t = new ArrayList();
								t.add(delta);
								deltaResultMap.put(parentId, t);
							}

						}					
					}
						
				}
									
			}
		}	// end of for
			
			//Handle inV CP

		for(String qid: inQuerySet){
				
			SubQuery squery = subQueryMap.get(qid);
			String counterQid= squery.getCounterQuery();
			String cp2Identifier= squery.getCP2Variable();
				
				
			if(counterQid.startsWith("G _ ")){				// Type 3
					
				String counterComp[]= counterQid.split(" _ ");
				String direction= counterComp[2];
				String tdbName= counterComp[1];
				
				String tdbEdgeId;
				tdbEdgeId = squery.checkCP2Condition(outV, tdbName, direction);
					
					
				if(tdbEdgeId.length() > 1){

					String parentId = squery.getParentQueryId();
					affectedQuerySet.add(parentId);
				
					/*
					 * Tracking the delta result which needs to be added in the final result
					 */
						
					ArrayList<String> temp = new ArrayList();
					temp.addAll(inQueryToDeltaResultMap.get(qid));
					String delta = new String();
			
					// TDB:resultItem1|propoly1, resultItem2|provPoly2:newEdgeDefId.TDBEdgeId
					delta = "TDB : ";
						
					for (String s : temp) {
						delta = delta.concat(s+"; ");
					}
						
					delta = delta.substring(0, delta.lastIndexOf(";"));
					delta = delta.concat(" : ").concat("e"+newEdgeDefId).concat(" . "+tdbEdgeId);
					
					if (deltaResultMap.containsKey(parentId)) {
						deltaResultMap.get(parentId).add(delta);
					} else {

						ArrayList<String> t = new ArrayList();
						t.add(delta);
						deltaResultMap.put(parentId, t);
					}
				}
			} else{
					
					if(counterQid.startsWith("Q : ")){			//Type 2

						String counterComp[]= counterQid.split(" : ");
						String counterQname= counterComp[1];
			
						if(outQuerySet.contains(counterQname)){
							
							String parentId = squery.getParentQueryId();
							outQuerySet.remove(counterQname);
							affectedQuerySet.add(parentId);
						
							/*
							 * COUNTER:resultItem1|provPoly1, resultItem2|ProvPoly2:resultItem4|provPoly4, resultItem5|provPoly5
							 */

							ArrayList<String> tempOut = new ArrayList();
							ArrayList<String> tempIn = new ArrayList();
							String delta = "COUNTER : ";
							
							tempIn.addAll(inQueryToDeltaResultMap.get(qid));
							tempOut.addAll(outQueryToDeltaResultMap.get(counterQname));
							
							for (String s : tempOut) {
								delta = delta.concat(s+"; ");
							}
							
							delta = delta.substring(0, delta.lastIndexOf(";"));
							delta = delta.concat(" : ");
							
							for (String s : tempIn) {
								delta = delta.concat(s+"; ");
							}
							
							delta = delta.substring(0, delta.lastIndexOf(";"));
							
							if (deltaResultMap.containsKey(parentId)) {
								deltaResultMap.get(parentId).add(delta);
							
							} else {
								ArrayList<String> t = new ArrayList();
								t.add(delta);
								deltaResultMap.put(parentId, t);
							}
						}
					} else{
						
						if(cp2Identifier.equals("NULL")){			// Type 0
					
							if(squery.checkCP2Condition(outV)){
								
								String parentId = squery.getParentQueryId();
								affectedQuerySet.add(parentId);
					
								ArrayList<String> temp = new ArrayList();
								temp.addAll(inQueryToDeltaResultMap.get(qid));
								String delta = new String();
								
								// ISOLATED:resultItem1|propoly1, resultItem2|provPoly2:newEdgeDefId
								delta = "ISOLATED : ";
								
								for (String s : temp) {
									delta = delta.concat(s+"; ");
								}
								
								delta = delta.substring(0, delta.lastIndexOf(";"));
								delta = delta.concat(" : ").concat("e"+newEdgeDefId);
								
								if (deltaResultMap.containsKey(parentId)) {
									deltaResultMap.get(parentId).add(delta);
								} else {
									ArrayList<String> t = new ArrayList();
									t.add(delta);
									deltaResultMap.put(parentId, t);
								}

							}
						}
						else{														// Type 1

							if(squery.checkCPCondition(inV) && squery.checkCP2Condition( outV)){

								String parentId = squery.getParentQueryId();
								affectedQuerySet.add(parentId);	

								ArrayList<String> temp = new ArrayList();
								temp.addAll(inQueryToDeltaResultMap.get(qid));
								String delta = new String();
								//CONNECTED:resultItem1|propoly1, resultItem2|provPoly2:newEdgeDefId
								delta = "CONNECTED : ";
								
								for (String s : temp) {
									delta = delta.concat(s+"; ");
								}
								
								delta = delta.substring(0, delta.lastIndexOf(";"));
								delta = delta.concat(" : ").concat("e"+newEdgeDefId);
								
								if (deltaResultMap.containsKey(parentId)) {
									deltaResultMap.get(parentId).add(delta);
								} else {
									ArrayList<String> t = new ArrayList();
									t.add(delta);
									deltaResultMap.put(parentId, t);
								}
							}						
						}
						
					}
									
				}
							
			}	// end of for
						
			return deltaResultMap;
		}

	private static Map<? extends String, ? extends String> readTag(Long vID) {

		HashMap<String, String> relationMap = new HashMap();
		/*
		 * Get vertex and get its "tag" property
		 */
		String tag = "";

		String uri = "";
		
		try(Transaction tx = db.beginTx()) {
			Node node = db.getNodeById(vID);
			uri = (String) node.getProperty("uri");
			
			if(node.hasProperty("tag")) {
				tag = (String) node.getProperty("tag");
			} else {
				relationMap.put("uri", uri);
				return relationMap;
			}
			tx.success();
			tx.close();
		}
		
		String tagComp[] = tag.split(" RELATION__");
	//	System.out.println(vID+"\t"+tag);
		relationMap.put("uri", uri);

		for(String str: tagComp) {

			if(str.length()>0) {
				String strComp[] = str.split("->");
				relationMap.put(strComp[0],strComp[1]);
			}
		}
		
		return relationMap;
	}
}
