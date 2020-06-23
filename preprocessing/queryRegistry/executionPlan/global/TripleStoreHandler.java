package preprocessing.queryRegistry.executionPlan.global;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map.Entry;

import main.ConfigurationReader;
import preprocessing.queryRegistry.executionPlan.local.AndOrGraph;
import preprocessing.queryRegistry.executionPlan.local.EAGVertex;
import preprocessing.queryRegistry.executionPlan.local.EquivalenceNode;
import preprocessing.queryRegistry.executionPlan.local.OperationNode;

public class TripleStoreHandler {

	HashMap<Integer, OperationNode> opNodeMap;
	HashMap<Integer, EquivalenceNode> eNodeMap;
	HashMap<String, BaseRelation> baseRelationMap;
	HashMap<String, InternalRelation> internalRelationMap;
	 ArrayList<String> rootNodesId;

	 
	 public TripleStoreHandler (AndOrGraph g, boolean toStore, String dataset) throws IOException {
	    
	
		 
		baseRelationMap = new HashMap();
		internalRelationMap = new HashMap();
		opNodeMap = new HashMap();
		eNodeMap = new HashMap();
		rootNodesId = new ArrayList();

		opNodeMap.putAll(g.getOpNodeList());
		eNodeMap.putAll(g.getENodeList());
		 
		findPaths(g);
		correctNaming();
	
		if (toStore) {
			
			 String baseRealtionMapPath = ConfigurationReader.get("BASE_NODE_MAP");
			 String internalRelationMapPath = ConfigurationReader.get("INTERNAL_NODE_MAP");

			 FileOutputStream file1 = new FileOutputStream(baseRealtionMapPath);
			 ObjectOutputStream object1 = new ObjectOutputStream(file1);
			 object1.writeObject(baseRelationMap);
			 object1.close();
			 
			 FileOutputStream file2 = new FileOutputStream(internalRelationMapPath);
			 ObjectOutputStream object2 = new ObjectOutputStream(file2);     
			 object2.writeObject(internalRelationMap);
			 object2.close();
		}


	}
	
	
	 private void correctNaming() {
		
		 HashMap<Integer, OperationNode> opNodeList = new HashMap(); 
		 HashMap<Integer, EquivalenceNode> eqNodeList = new HashMap();
		 
		 eqNodeList.putAll(eNodeMap);
		 opNodeList.putAll(opNodeMap);
		
		 /*
		  * Generate the name map
		  */
				 
		 HashMap<String, String> nameMapping = new HashMap();
		 
		 for (Entry<String, InternalRelation> entry: internalRelationMap.entrySet()) {
			 
			 String interId = entry.getKey();
			 InternalRelation interNode = entry.getValue();
			 
			 String leftChild = interNode.getLeftChildId();
			 String rightChild = interNode.getRightChildId();
			 
			 if (baseRelationMap.containsKey(leftChild)) {
				 String leftAttributeOld = interNode.getLeftAttribute().get(0);
				 
				 
				 String temp[] = leftChild.split("\\.");
				 String childId = null;
				 
				 if (temp.length == 2)
				  childId = temp[0]+"-"+temp[1];
				 else
					 childId = leftChild;
				 
				 String newAttributeName = baseRelationMap.get(leftChild).getRelation()+"-"+childId;
				 nameMapping.put(leftAttributeOld, newAttributeName);
			 }
			 
			 if (baseRelationMap.containsKey(rightChild)) {
				 String rightAttributeOld = interNode.getRightAttribute().get(0);
				 
				 String temp[] = rightChild.split("\\.");
				 String childId = null;
				 
				 if (temp.length == 2)
				  childId = temp[0]+"-"+temp[1];
				 else
					 childId = rightChild;
				
				 String newAttributeName = baseRelationMap.get(rightChild).getRelation()+"-"+childId;

				 nameMapping.put(rightAttributeOld, newAttributeName);
			 }
		 }
		 
		 /*
		  * Update all the names of attribute
		  */
		 
		 HashMap<String, InternalRelation> tempInternalRelationMap = new HashMap();
		 
		 for (Entry<String, InternalRelation> entry: internalRelationMap.entrySet()) {
			 
			 /*
			  * Update operands name
			  */
			 InternalRelation interNode = entry.getValue();
			 
			 String id = entry.getKey();
			 LinkedHashSet<String> leftAttributeList = new LinkedHashSet(interNode.getLeftOpAttribute());			 
			 LinkedHashSet<String> rightAttributeList = new LinkedHashSet(interNode.getRightOpAttribute());

			 interNode.getLeftOpAttribute().clear();
			 interNode.getRightOpAttribute().clear();
			 LinkedList<String> newLeftOperand = new LinkedList();
			 LinkedList<String> newRightOperand = new LinkedList();

			 String operation = interNode.getOperation();
			 
			 for (String attr: leftAttributeList) {
				 String newName = nameMapping.get(attr);
				 if(operation.startsWith("S"))
				 newLeftOperand.add("out-"+newName);
				 else if(operation.startsWith("O"))
				 newLeftOperand.add("in-"+newName);
				 else {
					 newLeftOperand.add("out-"+newName);
					 newLeftOperand.add("in-"+newName);

				 }
			 }
			 
			 for (String attr: rightAttributeList) {
				 String newName = nameMapping.get(attr);
				 
				 if(operation.endsWith("S"))
					 newRightOperand.add("out-"+newName);
				 else if(operation.endsWith("O"))
					 newRightOperand.add("in-"+newName);
				 else {
					 newRightOperand.add("out-"+newName);
					 newRightOperand.add("in-"+newName);
				 }
			 }
			 
			 interNode.setLeftOperandAttribute(newLeftOperand);
			 interNode.setRightOperandAttribute(newRightOperand);
			 
			 /*
			  * Set the list of all attributes
			  */
			 LinkedHashSet<String> left_temp = new LinkedHashSet(interNode.getLeftAttribute());
			 LinkedHashSet<String> right_temp = new LinkedHashSet(interNode.getRightAttribute());
			 interNode.getLeftAttribute().clear();
			 interNode.getRightAttribute().clear();
			 
			 
			 ArrayList<String> attributeListLeft = new ArrayList();
			 ArrayList<String> attributeListRight = new ArrayList();
			 
			 for (String attr: left_temp) {
				 
				 String newName = nameMapping.get(attr);
				 attributeListLeft.add("out-"+newName);
				 attributeListLeft.add("in-"+newName);
				 
				 
				 if(interNode.isRoot()) {
					 
					 for (String qId: interNode.gettempQVar().keySet()) {
						 
						String qTriple = interNode.gettempQVarEntry(qId,attr);
						String[] qComp = qTriple.split(" ");
						 	
						if (qComp[0].startsWith("?")) {
					 		interNode.addQVarMapEntry(qId,qComp[0], "out-"+newName);
					 	}
						 	
					 	if (qComp[2].startsWith("?")) {
					 		interNode.addQVarMapEntry(qId, qComp[2], "in-"+newName);
					 	}						 
					}
				}
			 }
			 
			for (String attr: right_temp) {

				String newName = nameMapping.get(attr);
				attributeListRight.add("out-"+newName);
				attributeListRight.add("in-"+newName);
				 				 
				for (String qId: interNode.gettempQVar().keySet()) {
					 
					String qTriple = interNode.gettempQVarEntry(qId,attr);
					String[] qComp = qTriple.split(" ");
					 	
					if (qComp[0].startsWith("?")) {
				 		interNode.addQVarMapEntry(qId,qComp[0], "out-"+newName);
				 	}
					 	
				 	if (qComp[2].startsWith("?")) {
				 		interNode.addQVarMapEntry(qId, qComp[2], "in-"+newName);
				 	}						 
				}
			}
			 
			interNode.addLeftAttributeList(attributeListLeft);
			interNode.addRightAttributeList(attributeListRight);
			tempInternalRelationMap.put(id, interNode);
		}
		 
		internalRelationMap.putAll(tempInternalRelationMap);
	}
	
	private void findPaths(AndOrGraph g) {
		
		 HashMap<Integer, ArrayList<String>> pathListOfBaseRelation = new HashMap();
		 HashSet<Integer> leaveNodesList = new HashSet();
		 leaveNodesList.addAll(g.getLevelENodeMap().get(1));

		 
		 for (Integer eId: leaveNodesList) {
			 pathListOfBaseRelation.put(eId, pathBuilder(eId, "E"+eId));			
		 }
		 
		 generateMongoPlan(g, pathListOfBaseRelation);
	}
	
	void generateMongoPlan (AndOrGraph g, HashMap<Integer, ArrayList<String>> pathCollection) {
		
		HashMap<String, HashSet<String>> relationToLeaveNode = new HashMap(); 
		HashMap<String, String> namingOfBaseRelation = new HashMap();
		
		/*
		 * Read each path and construct the mongoDB plan 
		 */
				
		for (Entry<Integer, ArrayList<String>> entry: pathCollection.entrySet()) {
			
			Integer baseId = entry.getKey();
			ArrayList<String> pathList = new ArrayList();
			pathList.addAll(entry.getValue());
			
			/*
			 * Fetch the relation name
			 */
			String relation = g.getENodeList().get(baseId).getEAGraph().getTripleIdMap().values().iterator().next().getRelation();
			int count = 1;
			HashSet<String> siblingLeaveNodeSet = new HashSet();
			HashMap<String,String> immediateParentMap = new HashMap();
			
			String objType = g.getENodeList().get(baseId).getAbstractGraph().getTripleList().get(0).getObjectType();
			String subType = g.getENodeList().get(baseId).getAbstractGraph().getTripleList().get(0).getSubjectType();
			String constraint = new String();
			boolean haveConstraint = false;
			
			if (subType.contains(" - ")) {
			  haveConstraint =true;
			  constraint = "sub".concat(" : "+subType.split(" - ")[1]);
			  
			} else if (objType.contains(" - ")) {
				  haveConstraint =true;
				  constraint = "obj".concat(" : "+objType.split(" - ")[1]);
			}
			
			for (String path: pathList) {
			
				String[] pathComp = path.split(":");
				String leaveId = Integer.toString(baseId).concat("."+Integer.toString(count));
				count++;
				siblingLeaveNodeSet.add(leaveId);
				
				BaseRelation leaveNode = new BaseRelation(leaveId, relation); 
				leaveNode.addPath(path);  			
			    
				if (haveConstraint) {
					leaveNode.setConstraint(constraint);
				}
				
				leaveNode.setParent(pathComp[2].substring(1));
				boolean isSingleChild = false;
				
				for (int i = 0; i < pathComp.length-2; i = i+ 2) {
									
					String childId = "";
					Integer childENodeId = Integer.parseInt(pathComp[i].substring(1));
					
					if (i == 0) {
						childId = leaveId;
					} else {
						childId = pathComp[i].substring(1);
					}
					
					if (pathComp[i+1].startsWith("O")) {
						
						Integer opId = Integer.parseInt(pathComp[i+1].substring(1));
						OperationNode opNode = g.getOpNodeList().get(opId);
						LinkedList<Integer> children = new LinkedList();
						children.addAll(opNodeMap.get(opId).getChildrenList());
						
						if (children.size() == 1) { // Two operand of same EAG
							
							isSingleChild = true;
							Integer eId = Integer.parseInt(pathComp[i+2].substring(1));
							boolean isRoot = g.getENodeList().get(eId).isRoot();
														
							String internalNodeId = Integer.toString(eId);
							InternalRelation interNode;
							
							if (internalRelationMap.containsKey(internalNodeId)) {

								 interNode = internalRelationMap.get(internalNodeId);
								
							} else {

								if (isRoot) {
									
									 interNode = new InternalRelation (internalNodeId, true);
									 rootNodesId.add(internalNodeId);
								}
								else
								 interNode = new InternalRelation (internalNodeId, false);
							}
							
							/*
							 * Add the operand's attribute list
							 */
							
							LinkedList<String> leftOperandAttribute = new LinkedList();
														
							for(EAGVertex e: opNode.getLeftOperand()) {
								leftOperandAttribute.addAll(opNode.getLeftNamingMap().get(e));		
							}
								
							if(interNode.getLeftOpAttribute().isEmpty())
								interNode.setLeftOperandAttribute(leftOperandAttribute);
							
							
							LinkedList<String> rightOperandAttribute = new LinkedList();
								
							for(EAGVertex e: opNode.getRightOperand()) {
											
								rightOperandAttribute.addAll(opNode.getRightNamingMap().get(e));			
							}

								
							if (interNode.getRightOpAttribute().isEmpty())
							interNode.setRightOperandAttribute(rightOperandAttribute);
							
							
							/*
							 * For naming the attributes correctly in format: S/O-RelationName-baseRelationId
							 */

							if (i == 0) {
								String rightLeaveId = Integer.toString(baseId).concat("."+Integer.toString(count));

								String leftOldName = new String();
								String leftAlternateName = new String();
								String rightOldName = new String();
								String rightAlternateName = new String();
								
								leftOldName = interNode.getLeftOpAttribute().get(0);
								rightOldName = interNode.getRightOpAttribute().get(0);
								leftAlternateName = leftOldName;
								rightAlternateName = rightOldName;
								String leftRelationName = leftAlternateName;
								String rightRelationName = rightAlternateName;
								
								leftAlternateName = leftRelationName.concat(" - ").concat(leaveId);
								namingOfBaseRelation.put(leftOldName.concat(" && ").concat(leaveId), leftAlternateName);
								
								rightAlternateName = rightRelationName.concat(" - ").concat(rightLeaveId);
								namingOfBaseRelation.put(rightOldName.concat(" && ").concat(rightLeaveId), rightAlternateName);
							}
							
							/*
							 * Add all the attribute names of this child; Depending upon the direction add it to the correct list
							 */
							
								
							if (isRoot) {
									
								for (Entry<String, HashMap<EAGVertex, ArrayList<String>>> entryVal : g.getENodeList().get(eId).getQVarMap().entrySet()) {
										
									String qId = entryVal.getKey();					
									HashMap<EAGVertex, ArrayList<String>> qMap = new HashMap();
										
									for (Entry<EAGVertex, ArrayList<String>> e1: entryVal.getValue().entrySet()) {
											
										ArrayList<String> tempList = new ArrayList();
										
										for (String s: e1.getValue()) {
											tempList.add(s);
										}
										
										qMap.put(e1.getKey(),tempList);
									}
										
										
									HashMap<EAGVertex, EAGVertex> leftVertexMapping = opNode.getLeftVertexMap();
									HashMap<EAGVertex, EAGVertex> rightVertexMapping = opNode.getRightVertexMap();
									HashSet<EAGVertex> qVarInLeftMap = new HashSet();
									qVarInLeftMap.addAll(qMap.keySet());
									qVarInLeftMap.retainAll(leftVertexMapping.keySet());
		
									HashSet<EAGVertex> qVarInRightMap = new HashSet();
									qVarInRightMap.addAll(qMap.keySet());
									qVarInRightMap.retainAll(rightVertexMapping.keySet());
			
									
									HashMap<String, String> leftTempMap = new HashMap();
									HashMap<String, String> rightTempMap = new HashMap();
										
										
									for (EAGVertex vertex: qVarInLeftMap) {
											
										HashSet<String> set = new HashSet();			
										set.addAll(opNode.getLeftNamingMap().get(leftVertexMapping.get(vertex)));
																			
										String s = set.iterator().next();
											
										if (set.isEmpty()) {	
											System.out.println("Inconsistency in EAG of root node");
											System.exit(0);
			
										} else{
										
											if (qMap.get(vertex).size() ==1) {
												System.out.print("\n\nError in amendment!!");
												System.out.println("ENode "+eId+"\t"+qMap.get(vertex).toString());
												System.exit(0);
											}
												
											leftTempMap.put(s, qMap.get(vertex).get(0));
											qMap.get(vertex).remove(0);
											
										}
											
									}
			
										
									for (EAGVertex vertex: qVarInRightMap) {
											
										HashSet<String> set = new HashSet();								
										set.addAll(opNode.getRightNamingMap().get(rightVertexMapping.get(vertex)));										
										String s = set.iterator().next();
											
										if (set.isEmpty()) {
											System.out.println("Inconsistency in EAG of root node");
											System.exit(0);
		
										} else{											
												rightTempMap.put(s, qMap.get(vertex).get(0));
				
										}
									}
										
									interNode.addtempQVarMap(qId, leftTempMap);
									interNode.addtempQVarMap(qId, rightTempMap);	
								}
								
								interNode.setProjection(g.getENodeList().get(eId).getProjection());
								
							} 
					
								
							ArrayList<String> leftChildAttributeList = new ArrayList();
							ArrayList<String> rightChildAttributeList = new ArrayList();
				
							for (HashSet<String> str: opNode.getLeftNamingMap().values()) {
								for (String s: str) {
									leftChildAttributeList.add(s);
								}	
							}
		
							if(interNode.getLeftAttribute().isEmpty())
								interNode.addLeftAttributeList(leftChildAttributeList);
								
								
							for (HashSet<String> str: opNode.getRightNamingMap().values()) {
								for (String s: str) {
									rightChildAttributeList.add(s);
								}
							}
						
							if(interNode.getRightAttribute().isEmpty())
								interNode.addRightAttributeList(rightChildAttributeList);

							if (isRoot) {
								interNode.addQueryId(g.getENodeList().get(eId).getQueryIdList());
							}
												
							/*
							 * Add the operation and operands Id
							 */
							String operation = opNodeMap.get(opId).getOperator();
							interNode.setOperation(operation);
							
							String rightLeaveId = Integer.toString(baseId).concat("."+Integer.toString(count));
							interNode.setLeftChild(childId);
							interNode.setRightChild(rightLeaveId);
								
							if (childId != leaveId) {
								internalRelationMap.get(childId).addParentId(internalNodeId);		
							}
							
							internalRelationMap.put(internalNodeId, interNode);
							
						} else { // normal case
							
							Integer eId = Integer.parseInt(pathComp[i+2].substring(1));
							boolean isRoot = g.getENodeList().get(eId).isRoot();
							String internalNodeId = Integer.toString(eId);
							InternalRelation interNode;
							
							if (internalRelationMap.containsKey(internalNodeId)) {

								 interNode = internalRelationMap.get(internalNodeId);								
							} else {

								if (isRoot) {
									
									 interNode = new InternalRelation (internalNodeId, true);
									 rootNodesId.add(internalNodeId);
								} else {
								 interNode = new InternalRelation (internalNodeId, false);
								}
							}
							
							/*
							 * Check if the childENode is left operand or right operand of the resultant internal node
							 */
							
							String direction = "";
							
							if (childENodeId.equals(children.get(0))){
								direction = "left";
							} else {
								direction = "right";
							}
							
							/*
							 * Add the operand's attribute list
							 */
							
							LinkedList<String> operandAttribute = new LinkedList();
							
							if (direction.equals("left")) {
								
								for(EAGVertex e: opNode.getLeftOperand()) {
									operandAttribute.addAll(opNode.getLeftNamingMap().get(e));									
								}
								
								if(interNode.getLeftOpAttribute().isEmpty())
									interNode.setLeftOperandAttribute(operandAttribute);
							
							} else {
								
								for(EAGVertex e: opNode.getRightOperand()) {	
									operandAttribute.addAll(opNode.getRightNamingMap().get(e));			
								}
								
								if (interNode.getRightOpAttribute().isEmpty())
									interNode.setRightOperandAttribute(operandAttribute);
							}
							
							/*
							 * For naming the attributes correctly in format: S/O-RelationName-baseRelationId
							 */
							if (i == 0) {
								
								String oldName = new String();
								String alternateName = new String();
								
								if (direction.equals("left")) {
									
									oldName = interNode.getLeftOpAttribute().get(0);
								} else {
									oldName = interNode.getRightOpAttribute().get(0);
								}
							
								alternateName = oldName;
								String relationName = alternateName;
								alternateName = relationName.concat(" - ").concat(leaveId);
								namingOfBaseRelation.put(oldName.concat(" && ").concat(leaveId), alternateName);
							}
							
							/*
							 * Add all the attribute names of this child; Depending upon the direction add it to the correct list
							 */
							ArrayList<String> childAttributeList = new ArrayList();
							
							if (direction.equals("left")) {
																
								if (isRoot) {
									
									for (Entry<String, HashMap<EAGVertex, ArrayList<String>>> entryVal : g.getENodeList().get(eId).getQVarMap().entrySet()) {
										
										String qId = entryVal.getKey();
										HashMap<EAGVertex, ArrayList<String>> qMap = entryVal.getValue();
										HashMap<EAGVertex, EAGVertex> vertexMapping = opNode.getLeftVertexMap();
										HashSet<EAGVertex> qVarInLeftMap = new HashSet();
										qVarInLeftMap.addAll(qMap.keySet());
										qVarInLeftMap.retainAll(vertexMapping.keySet());
			
										HashMap<String, String> tempMap = new HashMap();
										
										for (EAGVertex vertex: qVarInLeftMap) {
									
											HashSet<String> set = new HashSet();
											set.addAll(opNode.getLeftNamingMap().get(vertexMapping.get(vertex)));
											
											if (set.isEmpty()) {
												System.out.println("Inconsistency in EAG of root node");
												System.exit(0);
			
											} if(set.size()>1) {
											
												EAGVertex toFind = vertexMapping.get(vertex);
												ArrayList<EAGVertex> candidateEAG = new ArrayList();
												
												for (Entry<EAGVertex, EAGVertex> entry1: vertexMapping.entrySet()) {
													
													if(entry1.getValue().equals(toFind)) {
														candidateEAG.add(entry1.getKey());
													}
												}
												
												int count1 = 0;
												
												for (String str: set) {
													tempMap.put(str, qMap.get(candidateEAG.get(count1)).get(0));
													count1++;
												}
											} else{
											
												String s = set.iterator().next();
												tempMap.put(s, qMap.get(vertex).get(0));												
											}
											
										}
										
										interNode.addtempQVarMap(qId, tempMap);
									}
								
									interNode.setProjection(g.getENodeList().get(eId).getProjection());
								} 
									
								for (HashSet<String> str: opNode.getLeftNamingMap().values()) {
									for (String s: str) {
										childAttributeList.add(s);
									}				
								}
							
								if(interNode.getLeftAttribute().isEmpty())
									interNode.addLeftAttributeList(childAttributeList);
								
							} else { // if child is right
								
								if (isRoot) {
												
									for (Entry<String, HashMap<EAGVertex, ArrayList<String>>> entryVal : g.getENodeList().get(eId).getQVarMap().entrySet()) {
										
										String qId = entryVal.getKey();
										HashMap<EAGVertex, ArrayList<String>> qMap = entryVal.getValue();
										HashMap<EAGVertex, EAGVertex> vertexMapping = opNode.getRightVertexMap();
										HashSet<EAGVertex> qVarInRightMap = new HashSet();
										qVarInRightMap.addAll(qMap.keySet());
										qVarInRightMap.retainAll(vertexMapping.keySet());
			
										HashMap<String, String> tempMap = new HashMap();
										
										for (EAGVertex vertex: qVarInRightMap) {
										
											HashSet<String> set = new HashSet();
											set.addAll(opNode.getRightNamingMap().get(vertexMapping.get(vertex)));
											String s = set.iterator().next();
											
											if (set.isEmpty()) {
												System.out.println("Inconsistency in EAG of root node");
												System.out.println("Eid: "+eId+"\n QMap"+qMap.keySet().toString()+"\n VertexMap"+vertexMapping.keySet());
												System.exit(0);
			
											} if(set.size()>1) {
											
												EAGVertex toFind = vertexMapping.get(vertex);
												ArrayList<EAGVertex> candidateEAG = new ArrayList();
												
												for (Entry<EAGVertex, EAGVertex> entry1: vertexMapping.entrySet()) {
													
													if(entry1.getValue().equals(toFind)) {
														candidateEAG.add(entry1.getKey());
													}
												}
												
												int count1 = 0;
												
												for (String str: set) {
													tempMap.put(str, qMap.get(candidateEAG.get(count1)).get(0));
													count1++;
												}
											}else{
										
												tempMap.put(s, qMap.get(vertex).get(0));												
											}
											
										}
										
										interNode.addtempQVarMap(qId, tempMap);
									}

								interNode.setProjection(g.getENodeList().get(eId).getProjection());

								}
									
								for (HashSet<String> str: opNode.getRightNamingMap().values()) {
									for (String s: str) {
										childAttributeList.add(s);
									}
								}
				
							if(interNode.getRightAttribute().isEmpty())
								interNode.addRightAttributeList(childAttributeList);
						}
	
						if (isRoot) {
							interNode.addQueryId(g.getENodeList().get(eId).getQueryIdList());
						}
							

						/*
						 * Add the operation and operands Id
						 */
						String operation = opNodeMap.get(opId).getOperator();
						interNode.setOperation(operation);
							
						if (direction.equals("left")) {
							interNode.setLeftChild(childId);
						
						} else {
								interNode.setRightChild(childId);	
						}
							
						if (childId != leaveId) {
							internalRelationMap.get(childId).addParentId(internalNodeId);
						}
						
						internalRelationMap.put(internalNodeId, interNode);	
					}

				}		
					
			}
				
			baseRelationMap.put(leaveId, leaveNode);
				
			if (isSingleChild) {
					
				String rightLeaveId = Integer.toString(baseId).concat("."+Integer.toString(count));
				count++;
				siblingLeaveNodeSet.add(rightLeaveId);
				BaseRelation rightLeaveNode = new BaseRelation(rightLeaveId, relation); 
	

				if (haveConstraint) {
					rightLeaveNode.setConstraint(constraint);
				}
					
				rightLeaveNode.setParent(pathComp[2].substring(1));				
				baseRelationMap.put(rightLeaveId, rightLeaveNode);
			}		
		}
			
		relationToLeaveNode.put(relation, siblingLeaveNodeSet);		
	}	
}
	
	private ArrayList<String> pathBuilder (Integer eid, String str) {
		
		ArrayList<String> paths = new ArrayList();
				
		if (eNodeMap.get(eid).getParentList().isEmpty()) {
		
			paths.add(str);	
			return paths;
		} else {

			for (Integer opId: eNodeMap.get(eid).getParentList()) {
			
				LinkedHashSet parentList = new LinkedHashSet();
				parentList.addAll(opNodeMap.get(opId).getChildrenList());
				
				parentList.retainAll(opNodeMap.get(opId).getParentList());
				
				if (parentList.size() > 0) {
					System.exit(0);
				}

				String temp = "";

				if (opNodeMap.get(opId).getParentList().size() < 1) {
					System.exit(0);
				}
					
				Integer i = opNodeMap.get(opId).getParentList().iterator().next();
				paths.addAll(pathBuilder(i, str.concat(":O"+opId+":E"+i)));
			}
		}
		
		return paths;
	}
	
	public HashMap<String, BaseRelation> getBaseRelationMap() {
		return this.baseRelationMap;
	}
	
	public HashMap<String, InternalRelation> getInternalRelationMap() {
		return this.internalRelationMap;
	}
}
