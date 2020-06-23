package preprocessing.queryRegistry.executionPlan.global;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.syntax.ElementPathBlock;
import org.apache.jena.sparql.syntax.ElementVisitorBase;
import org.apache.jena.sparql.syntax.ElementWalker;

import preprocessing.queryRegistry.annotator.SubQuery;
import preprocessing.queryRegistry.executionPlan.local.AndOrGraph;
import preprocessing.queryRegistry.executionPlan.local.EquivalenceNode;
import preprocessing.queryRegistry.executionPlan.local.OperationNode;

import main.ConfigurationReader;
import main.GlobalDSReader;


public class InsertionPropogationWithSQL {

	HashMap<String, BaseRelation> baseRelationMap;
	HashMap<String, InternalRelation> internalRelationMap;
	HashMap<String, ArrayList<String>> relationToLeaveNodes;
	static HashSet<String> computedIntermediateId;
	HashSet<String> computedResultId;
	HashSet<String> deltaId;
	HashMap<String, String> joinNameMapping;
	BufferedWriter bw;
	String baseDelta;
	Statement statement = null;
	boolean overallMode = false;
	boolean isWasBornIn = false;
	String dataset;
	HashMap<String, SubQuery> subQueryMap;
	HashMap<String, HashMap<String,ArrayList<String>>> globalCPUpdateMap;
	GraphDatabaseService db;


	public InsertionPropogationWithSQL(HashMap<String, BaseRelation> baseRelationMap, HashMap<String, InternalRelation> internalRelationMap, String dataset) throws IOException, SQLException, ClassNotFoundException {
		
		this.joinNameMapping = new HashMap();
		this.baseRelationMap = new HashMap();
		this.baseRelationMap.putAll(baseRelationMap);
		this.relationToLeaveNodes = new HashMap();
		this.internalRelationMap = new HashMap();
		this.internalRelationMap.putAll(internalRelationMap);
		this.computedIntermediateId = new HashSet();
		this.computedResultId = new HashSet();
		this.deltaId = new HashSet();
		this.baseDelta = new String();
		this.overallMode = false;
		this.dataset = dataset;
		this.globalCPUpdateMap = new HashMap();
		/*
		 * Initialize the map which captures which leave nodes get activated 
		 * corresponding to the inserted relation fact
		 */
	
		for (Entry<String, BaseRelation> entry: baseRelationMap.entrySet()) {
			
			String relation = entry.getValue().getRelation();
			String id = entry.getKey();

			if (relationToLeaveNodes.containsKey(relation)) {
				relationToLeaveNodes.get(relation).add(id);

			} else {
				ArrayList<String> temp = new ArrayList();
				temp.add(id);
				relationToLeaveNodes.put(relation, temp);
			}
		}

		statement = GlobalDSReader.getSQLStatement();
		db = GlobalDSReader.getDBInstance();
		subQueryMap = new HashMap();
		subQueryMap.putAll(GlobalDSReader.getSubQueryMap());

	}
	
		
	public void updateSubQueryRevised(String subject, String predicate, String object, String state, int count, String edgeId) throws IOException, SQLException {
		
		/*
		 * From predicate choose the base relation nodes
		 * for each base relation find if the subCollection or the actual collection is needed
		 * based on that start the mongoDB joins
		 * 		At each join operation, check if the operand is a base relation or an internal relation
		 * 			If internal then evaluate that first and then proceed
		 */
	
			
		if(predicate.equals("rdf:type")) {
			predicate = "type";
		}
				
		ArrayList<String> affectedLeaveNodes = new ArrayList();
		affectedLeaveNodes.addAll(relationToLeaveNodes.get(predicate));
		baseDelta = "delta_"+predicate;
		
		/*
		 * Create a delta collection in the db
		 */
		
		String stmt = "Create table temp(`out` text,`in` text, poly VARCHAR(20));";
		statement.executeUpdate(stmt);
		String str = "insert into temp values (?, ?, ?)";
		
		PreparedStatement preparedStatement = GlobalDSReader.getSQLPreparedStatement(str);  
		preparedStatement.setString(1, subject);		
        preparedStatement.setString(2, object);		
        preparedStatement.setString(3, "e"+edgeId);		

        preparedStatement.executeUpdate();
        
        stmt = "create view `"+baseDelta+"` as select * from temp;";
        statement.executeUpdate(stmt); 
        
		String idStr = new String();
		int iterations  = 0;
		
		for (String id: affectedLeaveNodes) {
        
			if (id.contains(".")) {
				idStr = id.substring(0, id.indexOf("."));
			} else {
				idStr = id;
			}
			
			BaseRelation leaveNode = new BaseRelation();
			leaveNode = baseRelationMap.get(id);
			
			if (leaveNode.hasConstraint()) {
				
				String constraint = leaveNode.getConstraint();
				String constrComp[] = constraint.split(" : ");
        		String entity = constrComp[1];
        		
        		if (constrComp[0].equals("sub")) {
        			if(!entity.equals(subject)) {
        				continue;
        			}
        		}
        		else if(!entity.equals(object)) {
    				continue;	
        		}					
			}
			
			
			for( String path: leaveNode.getPathList()) {
				
				String direction = new String();
				String pathComp[] = path.split(":");
				LinkedList<String> pathCompList = new LinkedList();
				
				for (int i = 0; i < pathComp.length; i++) {
					
					if (pathComp[i].startsWith("E"))
						pathCompList.add(pathComp[i].substring(1));
				}
				
				String baseNodeId = pathCompList.getFirst();
				pathCompList.removeFirst();
				String previousDelta = new String();
				
				while (!pathCompList.isEmpty()) {
					
						
					String currentInterNodeId = pathCompList.getFirst();
					InternalRelation currentInterNode = internalRelationMap.get(currentInterNodeId);
					
					if (deltaId.contains(currentInterNodeId)) {
						pathCompList.removeFirst();
						previousDelta = currentInterNodeId;
						continue;
				    
				    } else {
						
						String leftChildId = currentInterNode.getLeftChildId();
						String rightChildId = currentInterNode.getRightChildId();
						String lIdStr = new String();
						String rIdStr = new String();
						
						if (leftChildId.contains(".")) {
							lIdStr = leftChildId.substring(0, leftChildId.indexOf("."));

						} else {
							lIdStr = leftChildId;
						}

						if (rightChildId.contains(".")) {
							rIdStr = rightChildId.substring(0, rightChildId.indexOf("."));

						} else {
							rIdStr = rightChildId;
							
						}			
						
						if (lIdStr.equals(previousDelta))
							direction = "left";
						else if (lIdStr.equals(idStr))
							direction = "left";
						else if (rIdStr.equals(previousDelta)) 
							direction = "right";
						else if (rIdStr.equals(idStr))
							direction = "right";		
						
						iterations++;
					
						/*
						 * check if it is the last element of the path, if it is send signal to count the final result size
						 * 
						 */
						boolean isLast = false;
						
						if(pathCompList.size() == 1)
							isLast = true;
						
						int status = joinWithAggregateNoSwap(currentInterNode.getId(), true, direction);

						if (status != -1) {
							previousDelta = currentInterNodeId;
						 
						 if (currentInterNode.isRoot)
						 	computedResultId.add(currentInterNodeId);
						}
					   else {
							break;
						}
										
						pathCompList.removeFirst();
					
					} //end of else
				} // end of while		
				
			  } // end of path for loop	
				
			} // end of affected leaves for loop	
	}

	
	public void updateCPWithNeo(HashSet<String> computedResultId) throws SQLException, UnsupportedEncodingException {
		
		/*
		 * for each item in the delta_resultId view
		 * 		Find the CP and result_item and lookup for the vertexId of CP
		 * 		Update that 
		 */
		
		long updateMapConsSt = System.nanoTime();
		
		for (String qId: computedResultId) {
		
			ArrayList<String> resultVar = new ArrayList();
			HashMap<String, HashMap<String,ArrayList<String>>> tempMap = new HashMap();
			tempMap.putAll(fetchCPUpdates(qId));
			
			for (Entry<String, HashMap<String, ArrayList<String>>> entry: tempMap.entrySet()) {
				
				String key = entry.getKey();
				HashMap<String, ArrayList<String>> valMap = new HashMap();
				valMap.putAll(entry.getValue());
				
				if (globalCPUpdateMap.containsKey(key)) {
					
					for (Entry<String, ArrayList<String>> e : valMap.entrySet()) {
						String relation = e.getKey();
						
						if (globalCPUpdateMap.get(key).containsKey(relation)) {
							globalCPUpdateMap.get(key).get(relation).addAll(e.getValue());
						} else {
							
							globalCPUpdateMap.get(key).put(relation, e.getValue());
						}
						
					}
					
				} else {
					globalCPUpdateMap.put(key, valMap);
				}
			}
			
		} // Information of all the CP accross updated subqueries is gathered
		

		long updateMapConsEt = System.nanoTime();
		

		/*
		 * change the globalCPUpdateMap keys from CP value to the CP node id
		 */
		
		HashMap<String, HashMap<String, ArrayList<String>>> tempMap = new HashMap();
		
		tempMap.putAll(globalCPUpdateMap);
		
		globalCPUpdateMap.clear();
		
		String labelVal = "";
		
		long findVIdSt = System.nanoTime();
		
		for (Entry<String, HashMap<String, ArrayList<String>>> entry: tempMap.entrySet()) {
			
			labelVal = entry.getKey();
			String vid = GlobalDSReader.getVertexId(labelVal);
			
			HashMap<String, ArrayList<String>> tMap = new HashMap();
			tMap.putAll(entry.getValue());
			globalCPUpdateMap.put(vid, tMap);
			
		}
		
		long findVIdEt = System.nanoTime();
		
		long annotateSt = System.nanoTime();
		
		HashMap<Long , HashMap<String, String>> allCPTagMap = new HashMap();
		allCPTagMap.putAll(readTag(globalCPUpdateMap.keySet()));
		HashMap<Long , HashMap<String, String>> updatedCPTag = new HashMap();

		for (Entry<String, HashMap<String, ArrayList<String>>> entry: globalCPUpdateMap.entrySet()) {
			
		
			String vid = entry.getKey();			
			Long vID = Long.parseLong(vid);
		
			HashMap<String, String> tagMap = new HashMap();
			tagMap.putAll(allCPTagMap.get(vID));
			HashMap<String, ArrayList<String>> affectedRelationMap = new HashMap();			
			affectedRelationMap.putAll(entry.getValue());
			boolean isNodeCP = false;
			ArrayList<String> affectedRelations = new ArrayList();
			affectedRelations.addAll(affectedRelationMap.keySet());
			
			for(String str: affectedRelations) {
				
				if(tagMap.containsKey(str)) {
										
					String qListVal = tagMap.get(str);
					String newQList = qListVal;
					
					for(String newEntry: affectedRelationMap.get(str)) {
						
						String newEntryComp[] = newEntry.split(" \\| ");
						String temp = newEntryComp[0]+" | "+newEntryComp[1]+" | "+newEntryComp[2]+" | ";
						
						if (!newQList.contains(temp)) {
							
							if (newQList.length() > 0)
								newQList = newQList.concat("; "+newEntry);
							else
								newQList = newEntry;
						
						} else {
							
							String temp1 = temp.replaceAll("[\\<\\(\\[\\{\\\\\\^\\-\\=\\$\\!\\|\\]\\}\\)\\?\\*\\+\\.\\>]", "\\\\$0"); 
							String tempComp[] =newQList.split(temp1);

							String oldPoly = "";
							String newPoly = "";
							
							if (tempComp[1].contains(";")) {
								oldPoly = tempComp[1].substring(0, tempComp[1].indexOf(";"));
							} else {
								oldPoly = tempComp[1];
							}
							
							newPoly = oldPoly.concat("+").concat(newEntryComp[3]);
							String oldList = temp+oldPoly;
							String newList = temp+newPoly;
							newQList = newQList.replace(oldList, newList);

							}
						}
						
					
					if (newQList.length() == 0)
						System.out.println("Here is the error");
	        		
					tagMap.put(str, newQList);
	        		affectedRelationMap.remove(str);					
				}
			}
			
			if (affectedRelationMap.size() > 0) {
	
				for (Entry<String, ArrayList<String>> e : affectedRelationMap.entrySet()) {
					
					String qListVal = "";
					
					for (String s: e.getValue()) {
						qListVal = qListVal.concat(s+"; ");
					}
					
					qListVal = qListVal.substring(0, qListVal.lastIndexOf(";"));
					String val = e.getKey();
	        		tagMap.put(val, qListVal);
	        			        		
				}
			}
		
			updatedCPTag.put(vID,tagMap);
		} // out of cp	
		
		
		updateTag(updatedCPTag);

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
		tx.close();
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
	tx.close();

	}	
}


	
	
	private Map<? extends String, ? extends HashMap<String, ArrayList<String>>> fetchCPUpdates(
			String interNodeId) throws SQLException {
		
			 
			InternalRelation subQueryNode = internalRelationMap.get(interNodeId);
			HashMap<String, HashMap<String, String>> CPToResultMap = new HashMap();
			HashMap<String, HashMap<String, String>> QVarToAttributeMap = new HashMap();
			
			QVarToAttributeMap.putAll(subQueryNode.getQVarMap());
			
			String CPVar, CP2Var;
			ArrayList<String> resultVar;
			String CPViewVar, CP2ViewVar = "";
			HashMap<String, String> resultViewVar = new HashMap();			
	
			HashMap<String, HashMap<String, HashMap<String, String>>> CPMetaCollection = new HashMap();	
			
			/*
			 * Fetch all the result corresponding to the subQueryNode
			 */
			
			String qStmt = "SELECT * FROM delta_"+interNodeId+";";
			
	        ResultSet rs = statement.executeQuery(qStmt);

	        while(rs.next()) {
	        	
	        	String poly = rs.getString("poly");
	        	
	        	HashMap<String, String> attributeValueMap = new HashMap();
	        	
	        	for (Entry<String, HashMap<String, String>> entry: QVarToAttributeMap.entrySet()) {
	        		
	        		String qID = entry.getKey();
	        		String CPVal, CP2Val;
	        		String resultVal = new String();
	        		
					SubQuery subquery  = subQueryMap.get(qID);
	        		CPVar = subquery.getCP1Variable();
	        		CP2Var = subquery.getCP2Variable();
	        		resultVar = new ArrayList();
	        		resultVar.addAll(Arrays.asList(subquery.getResultVariable().split(", ")));
	        		
	        		CPViewVar = entry.getValue().get("?"+CPVar);	
	        		CPViewVar = CPViewVar.replace("-", "_");
	        		
	        		if (!CP2Var.equals("NULL")) {
		        		CP2ViewVar = entry.getValue().get("?"+CP2Var);
		        		CP2ViewVar = CP2ViewVar.replace("-", "_");
	        		}

	        		String str = "";
	        		for (String s: resultVar) {
	        			str = entry.getValue().get("?"+s);
	        			str = str.replace("-", "_");
	        			resultViewVar.put(str,s);
	        		}
	        		
	        		/*
	        		 * Get values of CP, PC2 and result items
	        		 */
	        
	        		if (attributeValueMap.containsKey(CPViewVar)) 	{
	        			CPVal = attributeValueMap.get(CPViewVar);
	        		} else {
	        			CPVal = rs.getString(CPViewVar);
	        			attributeValueMap.put(CPViewVar, CPVal);
	        		}
	        		
	        		if (!CP2Var.equals("NULL")) {
	        			
	        			if (attributeValueMap.containsKey(CP2ViewVar)) 	{
		        			CP2Val = attributeValueMap.get(CP2ViewVar);
		        		} else {
		        			CP2Val = rs.getString(CP2ViewVar);
		        			attributeValueMap.put(CP2ViewVar, CP2Val);
		        		}
		        			
	        		} else {
	        			CP2Val = "NULL";
	        		}
	        		
	        		
	        		for (String attr : resultViewVar.keySet()) {
	        			
	         			if (attributeValueMap.containsKey(attr)) 	{
		        			resultVal = resultVal.concat(resultViewVar.get(attr)+" = "+attributeValueMap.get(attr)+", ");  // TODO: Check the format of the result stored in the CP resultVar=resultVal or just resultVal
		        		
	         			} else {
		        			String t = rs.getString(attr);
		        			attributeValueMap.put(attr, t);
		        			resultVal = resultVal.concat(resultViewVar.get(attr)+" = "+t+", ");  // TODO: Check the format of the result stored in the CP resultVar=resultVal or just resultVal
				        }
	        		}
	        		
	        		resultVal = resultVal.substring(0, resultVal.lastIndexOf(","));
	        
	        		
	        		if (CPMetaCollection.containsKey(CPVal)) {
	        			
	        			HashMap<String, HashMap<String, String>> tempQMap = new HashMap();
	        			tempQMap.putAll(CPMetaCollection.get(CPVal));
	        			
	        			if (tempQMap.containsKey(qID)) {
	        				
	        				HashMap<String, String> tempResultMap = new HashMap();
	        				tempResultMap.putAll(tempQMap.get(qID));
	        				
	        				 
	        					
	        					if (tempResultMap.containsKey(resultVal)) {
	        						
	        						String tempPoly = tempResultMap.get(resultVal);
	        						tempPoly = tempPoly.concat("+"+poly);
	        						tempResultMap.put(resultVal, tempPoly);
	        						tempQMap.put(qID, tempResultMap);
	        						CPMetaCollection.put(CPVal, tempQMap);
	        					
	        					} else {
	        						tempResultMap.put(resultVal, poly);
	        						tempQMap.put(qID, tempResultMap);
	        						CPMetaCollection.put(CPVal, tempQMap);
	        					
	        					}
	        				
	        			} else {
	        				
	        				HashMap<String, String> tempRMap = new HashMap();
	        				tempRMap.put(resultVal, poly);
	        				tempQMap.put(qID, tempRMap);
	        				CPMetaCollection.put(CPVal, tempQMap);
	        			}

	        		} else {
	        			
	        			HashMap<String, String> tempRMap = new HashMap();
        				tempRMap.put(resultVal, poly);
        				HashMap<String, HashMap<String, String>> tempQMap = new HashMap();
        				tempQMap.put(qID, tempRMap);
        				CPMetaCollection.put(CPVal, tempQMap);	
	        		}
	        	} // For each query
	        } // For each result in the delta
	       
			HashMap<String, HashMap<String, ArrayList<String>>> result = new HashMap();
			
			for (Entry<String, HashMap<String, HashMap<String, String>>> entry: CPMetaCollection.entrySet()) {
				
				String CP = entry.getKey();
				HashMap<String, HashMap<String, String>> qMap = new HashMap();
				qMap.putAll(entry.getValue());
				
				for (Entry<String, HashMap<String, String>> e :qMap.entrySet()) {
					
					String qId = e.getKey();
					HashMap<String, String> resultAndPoly = new HashMap();
					resultAndPoly.putAll(e.getValue());
					
					String direction = subQueryMap.get(qId).getDirection();
					String expectedRelation = subQueryMap.get(qId).getExpectedRelation();
					ArrayList<String> qList = new ArrayList();
					
					for (Entry<String, String> valPoly: resultAndPoly.entrySet()) {
						
						String qListVal = qId.concat(" | "+direction+" | "+valPoly.getKey()+" | "+valPoly.getValue());
						qList.add(qListVal);
					}
					
					if (result.containsKey(CP)) {
						
						if (result.get(CP).containsKey(expectedRelation)) {
							result.get(CP).get(expectedRelation).addAll(qList);
						} else {
							
							result.get(CP).put(expectedRelation, qList);
						}
					} else {
							HashMap<String, ArrayList<String>> temp = new HashMap();
							temp.put(expectedRelation, qList);
							result.put(CP, temp);
						}
				}
			}
				
		return result;
	}


	public void cleanUp() throws SQLException {

		/*
		 * Delete all the collections whose name starts with "delta" or "temp"
		 */
		
		String stmt = new String();
		
		for (String id: computedIntermediateId) {	
			stmt = "drop view `temp_"+id+"`;";
			statement.executeUpdate(stmt);
		}
				
		for (String id: deltaId) {
			stmt = "drop view `delta_"+id+"`;";
			statement.executeUpdate(stmt);
		
		}
		    	
		stmt = "drop view `"+baseDelta+"`;";
		statement.executeUpdate(stmt);
		statement.executeUpdate("drop table temp;");
    	
    	deltaId.clear();
    	computedIntermediateId.clear();
    	computedResultId.clear();
    	globalCPUpdateMap.clear();

    	
	}
	

	private Integer joinWithAggregateNoSwap( String id, boolean isDelta, String operandSide) throws IOException, SQLException {
	
	
		/*
		 * Check operand node type -- base or internal
		 * If base 
		 * 		check if complete relation or subrelation
		 * 			if sub relation -- evaluate it
		 * If internal 
		 * 		check if it is in computedIntermediateId set
		 * 		If yes them proceed to join
		 * 		else compute it recursively (keep adding the ids to the computedIntermediateIds set) 
		 */

		
		InternalRelation parent = internalRelationMap.get(id);
		String leftOperandId = parent.getLeftChildId();
		String rightOperandId = parent.getRightChildId();
		boolean isLeftOpDelta = false;
		boolean isRightOpDelta = false;
		
		
		boolean isLeftOpBase = false, isRightOpBase = false;
	
		String leftCollection = new String();
		String rightCollection = new String();
		String resultingCollection = new String();
					
		if (baseRelationMap.containsKey(leftOperandId)) {
			isLeftOpBase = true;

		} else {
			isLeftOpBase = false;
		}
		
		if (baseRelationMap.containsKey(rightOperandId)) {
			isRightOpBase = true;
		
		} else {
			isRightOpBase = false;
		}

		
		if(isDelta) {
			
			if (operandSide.equals("left"))
				isLeftOpDelta = true;
			else if (operandSide.equals("right"))
				isRightOpDelta = true;
			
			if(isLeftOpDelta && isRightOpDelta) {
				System.out.println("Both delta ops!!");
				System.exit(0);
				isRightOpDelta = false;
			}
			if (!(isLeftOpDelta || isRightOpDelta)) {
				System.out.println("Error: Operands are not appropriate for delta resultant");
				System.exit(0);
			}
			else {
			
				if(deltaId.contains(id)) {
					return 1;	
				}
				
				if (!isLeftOpDelta) {
					
					if(!isLeftOpBase) {
					
						if(!computedIntermediateId.contains(leftOperandId)) {
					
							if (joinWithAggregateNoSwap(leftOperandId, false, null) == -1)
								return -1;
						}
						
						leftCollection = "temp_"+leftOperandId;
						
					} else {
						leftCollection = baseRelationMap.get(leftOperandId).getRelation();
					}
				} else {
					/*
					 * left relation name as delta_leftOpId
					 */
					if (isLeftOpBase)
						leftCollection = "delta_"+baseRelationMap.get(leftOperandId).getRelation();
					else
						leftCollection = "delta_"+leftOperandId;
				}
				
				if(!isRightOpDelta) {
					
					if(!isRightOpBase) {
						if (!computedIntermediateId.contains(rightOperandId)) {

							if (joinWithAggregateNoSwap(rightOperandId, false, null) == -1)
								return -1;
						}
						
						rightCollection = "temp_"+rightOperandId;
						
					} else {
						rightCollection = baseRelationMap.get(rightOperandId).getRelation();
					}
				} else{
					
					if(isRightOpBase)
						rightCollection = "delta_"+baseRelationMap.get(rightOperandId).getRelation();
					else
						rightCollection = "delta_"+rightOperandId;
				}
				
				resultingCollection = "delta_"+id;
				
			}			
		} // if delta true condition over
		else {
			
			if (computedIntermediateId.contains(id)) {
				return 1;
			}
			
			if (!isLeftOpBase) {
				
				if (!computedIntermediateId.contains(leftOperandId)) {
					
				    if(joinWithAggregateNoSwap(leftOperandId, false, null) == -1)
				    	return -1;

				}
				leftCollection = "temp_"+leftOperandId;

			} else {
				/*
				 *  base relation is left operand 
				 */
				leftCollection = baseRelationMap.get(leftOperandId).getRelation();

			}
			
			if(!isRightOpBase) {
				
				if (!computedIntermediateId.contains(rightOperandId)) {

				     if(joinWithAggregateNoSwap(rightOperandId, false, null) == -1)
				    	 return -1;
				}
				rightCollection = "temp_"+rightOperandId;

			} else {
				rightCollection = baseRelationMap.get(rightOperandId).getRelation();

			}
			
			resultingCollection = "temp_"+id;
		}
		
		HashMap<String, String> leftOpProjection = new HashMap();
		HashMap<String, String> rightOpProjection = new HashMap();
		
		HashMap<String, String> leftOpConstraint = new HashMap();
		HashMap<String, String> rightOpConstraint = new HashMap();
		
		String leftOp = new String();
		String rightOp = new String();
		
		boolean multipleJoins = false;
		
        /*
         * Adjusting left and right operand to apply selection (constraint) before the
         * join operation
         */
        
        boolean leftHasConstraint = false;
        boolean rightHasConstraint = false;
        
        if(isLeftOpBase) {
        	boolean hasConstraint = baseRelationMap.get(leftOperandId).hasConstraint();  
        	
        	if (hasConstraint) {
        		String constr = baseRelationMap.get(leftOperandId).getConstraint();
        		String constrComp[] = constr.split(" : ");
        		String entity = new String();
        		
        		if (constrComp[0].equals("sub"))
        			entity = "out";
        		else
        			entity = "in";
        		
        		leftOpConstraint.put(entity, constrComp[1]);
        		
        		
        		leftHasConstraint = true;
        	}
        }
        
        if(isRightOpBase) {
        	boolean hasConstraint = baseRelationMap.get(rightOperandId).hasConstraint();  
        	
        	if (hasConstraint) {
        		String constr = baseRelationMap.get(rightOperandId).getConstraint();
        		String constrComp[] = constr.split(" : ");
        		String entity = new String();
        		
        		if (constrComp[0].equals("sub"))
        			entity = "out";
        		else
        			entity = "in";
        		
        		rightOpConstraint.put(entity, constrComp[1]);
                        		
        		rightHasConstraint = true;
        	}
        }
        
        
        
		if(!multipleJoins) {
			
			 if(!(leftHasConstraint) && rightHasConstraint) {
				 
				 String left = internalRelationMap.get(id).getLeftOpAttribute().get(0);
				 String right = internalRelationMap.get(id).getRightOpAttribute().get(0);
				 String leftMatchOp = new String();
				 String rightMatchOp = new String();
	         
				 if(isLeftOpBase) {
					 leftMatchOp = left.split("-")[0]; 
				 }
				 else{
					 leftMatchOp = left;
				 }
	        
				 if(isRightOpBase) {
					 rightMatchOp = right.split("-")[0]; 
				 }
				 else{
					 rightMatchOp = right;
				 }
	         
				 leftOp = leftMatchOp;
				 rightOp = rightMatchOp;
				 
				 leftOp = leftOp.replace("-", "_");
				 rightOp =  rightOp.replace("-", "_");
 
	
				 
			 } else {
			 
				 String left = internalRelationMap.get(id).getLeftOpAttribute().get(0);
				 String right = internalRelationMap.get(id).getRightOpAttribute().get(0);
				 String leftMatchOp = new String();
				 String rightMatchOp = new String();
	         
				 if(isLeftOpBase) {
					 leftMatchOp = left.split("-")[0]; 
				 }
				 else{
					 leftMatchOp = left;
				 }
	        
				 if(isRightOpBase) {
					 rightMatchOp = right.split("-")[0]; 
				 }
				 else{
					 rightMatchOp = right;
				 }
				 
				 leftOp = leftMatchOp;
				 rightOp = rightMatchOp;
				 
				 leftOp = leftOp.replace("-", "_");
				 rightOp =  rightOp.replace("-", "_");
				 
			 }  	
		} 
         
		
	        /*
	         * To project the attributes of left and right operand
	         */
	        
	    ArrayList<String> rightAttr = new ArrayList();
	    ArrayList<String> leftAttr = new ArrayList();

	    rightAttr.addAll(internalRelationMap.get(id).getRightAttribute());
	    leftAttr.addAll(internalRelationMap.get(id).getLeftAttribute());
	        
	     /*
	      * Use projection to renmae the attributes (of right operand) and to project relevant fields
          */
	           
        String altAttr = new String();


	    for (String attr: rightAttr) {
		
        	if(isRightOpBase) 
	     		altAttr = attr.split("-")[0];
	       	else
        		altAttr = attr;
		        	
		    altAttr = altAttr.replaceAll("-", "_");
		   	attr = attr.replaceAll("-", "_");

		   	rightOpProjection.put(attr, altAttr);
        }
		        
		   
		for (String attr: leftAttr) {
		        	
		    if(isLeftOpBase) 
		   		altAttr = attr.split("-")[0];
		    else
		   		altAttr = attr;

		   	altAttr = altAttr.replaceAll("-", "_");
		   	attr = attr.replaceAll("-", "_");
	     	leftOpProjection.put(attr, altAttr);
	    }
        	
	     
		        
	    String viewStmt = "Create view `"+resultingCollection+"` as select";    
	    String leftProjectStmt = new String();
	        
	    for (Entry<String, String> entry: leftOpProjection.entrySet()) {
	        	
	      	leftProjectStmt = leftProjectStmt.concat(" leftR."+entry.getValue()+" as `"+entry.getKey()+"`,");	        	
	    }

	    String rightProjectStmt = new String();
	        
	    for (Entry<String, String> entry: rightOpProjection.entrySet()) {
	        	
	       	rightProjectStmt = rightProjectStmt.concat(" rightR."+entry.getValue()+" as `"+entry.getKey()+"`,");
	    }
	        
	    String polyStmt = " CONCAT_WS('', leftR.poly, rightR.poly) as `poly`";
	    String project = leftProjectStmt.concat(rightProjectStmt).concat(polyStmt);
		String fromStmt = " `"+leftCollection+"` as leftR, `"+rightCollection+"` as rightR";
	    String whereStmt = new String();
	        
	    for (Entry<String, String> entry: leftOpConstraint.entrySet()) {
	        	
	       	whereStmt = whereStmt.concat(" leftR."+entry.getKey()+" = \""+entry.getValue()+"\"");
	    }
	        
	        
	    for (Entry<String, String> entry: rightOpConstraint.entrySet()) {
	        
	       	if (whereStmt.length() > 0)
	        	whereStmt = whereStmt.concat(" AND rightR."+entry.getKey()+" = \""+entry.getValue()+"\"");
	       	else
	       		whereStmt = whereStmt.concat(" rightR."+entry.getKey()+" = \""+entry.getValue()+"\"");
	    }
	        
	    String joinStmt = new String();
	    joinStmt = " leftR."+leftOp+" = rightR."+rightOp+";";
	        
	    if (whereStmt.length() > 0)
	       	whereStmt = whereStmt.concat(" AND"+joinStmt);
	    else
	       	whereStmt = whereStmt.concat(joinStmt);
	        
	    viewStmt = viewStmt.concat(project).concat(" FROM").concat(fromStmt).concat(" WHERE").concat(whereStmt);
	    statement.executeUpdate(viewStmt);
	        
	    /*
	     * Try this query to find if table is non-empty
	     *  select count(*) from (select * from worksFor limit 1) as A;
	     */
	       
	    long count = -1, rightCount = -1, leftCount = -1;
	    String countStmt = "Select count(*) as rowcount from "+resultingCollection+";";
	    countStmt = "SELECT EXISTS (SELECT 1 FROM "+resultingCollection+" limit 1 ) as rowcount;";

	    ResultSet rs = statement.executeQuery(countStmt);   
	    String c = new String();
	       
	        while(rs.next())
	         c = rs.getString("rowcount");
	        
	         count = Long.parseLong(c);
	        
	       if(overallMode) {
	        	
	 	        countStmt = "select count(*) as rowcount from (select * from "+leftCollection+" limit 1) as A;";
	 	    	countStmt = "SELECT EXISTS (SELECT 1 FROM "+leftCollection+" limit 1 ) as rowcount;";

            	rs = statement.executeQuery(countStmt);  
	         
	       		 while(rs.next())
	         		c = rs.getString("rowcount");
	         
	         	leftCount = Long.parseLong(c);

	 	        countStmt = "select count(*) as rowcount from (select * from "+rightCollection+" limit 1) as A;";
		        countStmt = "SELECT EXISTS (SELECT 1 FROM "+rightCollection+" limit 1 ) as rowcount;";

	    	    rs = statement.executeQuery(countStmt);

	        	while(rs.next())
	          		c = rs.getString("rowcount");
	         
	          	rightCount = Long.parseLong(c);
	        }
	       	

	        if (count == 0 ) {
	        	statement.executeUpdate("Drop view "+resultingCollection+";");
	        	return -1;
	        }
	

	        if (isDelta) {
	        	deltaId.add(id);
	        } else {
	        	computedIntermediateId.add(id);
	        }

	        return 0;		
	}


	public HashSet<String> getComputedResults() {
		return computedResultId;
	}

	public HashMap<String, HashMap<String,ArrayList<String>>> getGlobalCPMap() {
		return globalCPUpdateMap;
	}

	public void updateEdgeToCPMap(
			HashMap<String, HashMap<String,ArrayList<String>>> globalCPMap) throws IOException, ClassNotFoundException {

		
		HashMap<String, HashSet<String>> map = new HashMap();
		HashMap<String, HashSet<String>> tempMap = new HashMap();
		map.putAll(GlobalDSReader.getEdgeToCPMap());
	
		 for (Entry<String, HashMap<String,ArrayList<String>>> entry: globalCPMap.entrySet()) {
			 
			 String cpId = entry.getKey();
			 
			 for (Entry<String, ArrayList<String>> e : entry.getValue().entrySet()) {
				 
				 String expectedRelation = e.getKey();
				 HashSet<String> edgeCollection = new HashSet();
				 
				 for (String str : e.getValue()) {
					 
					 String strComp[] = str.split(" \\| ");
					 String poly = strComp[3];
					 
					 if (poly.contains("+")) {
						 String addends[] = poly.split("\\+");
						 
						 for (int i = 0; i < addends.length; i++) {
							 String edges[] = addends[i].split("e");
							 
							 for (String edge: edges) {
								 edgeCollection.add(edge);
							 }
						 }
					 } else {
						 String edges[] = poly.split("e");
						 
						 for (String edge: edges) {
							 edgeCollection.add(edge);
						 }
					 }
				 }
				 
				 /*
				  * Add entry for each edge in the edgeCollection
				  */
				 for (String edgeId: edgeCollection) {
					 
					 if (map.containsKey(edgeId)) {
						 map.get(edgeId).add(cpId+" : "+expectedRelation);
						 tempMap.put(edgeId, map.get(edgeId));
					 } else {
						 HashSet<String> t = new HashSet();
						 t.add(cpId.concat(" : "+expectedRelation));
						 map.put(edgeId, t);
						 tempMap.put(edgeId, t);
					 }
				 }
			 }
		 }

		 GlobalDSReader.addToEdgeToCPMap(tempMap);
	}

	public void closeConnections() throws SQLException {

		statement.close();
	}

}
