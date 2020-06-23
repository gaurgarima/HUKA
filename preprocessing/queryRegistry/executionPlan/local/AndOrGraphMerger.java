package preprocessing.queryRegistry.executionPlan.local;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Random;
import java.util.Set;

import preprocessing.queryRegistry.executionPlan.global.BaseRelation;
import preprocessing.queryRegistry.executionPlan.global.InternalRelation;
import preprocessing.queryRegistry.executionPlan.global.TripleStoreHandler;
import preprocessing.estimator.CardinalityEstimator;
import preprocessing.estimator.ModifiedPredicate;


import java.util.Map.Entry;

public class AndOrGraphMerger {

	ArrayList<AndOrGraph> graphCollection;
	HashMap<Integer, Integer> equiNodeMap;
	HashMap<Integer, Integer> queryRootMap;
	
	public AndOrGraphMerger (ArrayList<AndOrGraph> graphList) {
		
		this.graphCollection = new ArrayList();
		this.graphCollection.addAll(graphList);
		this.equiNodeMap = new HashMap();
		this.queryRootMap = new HashMap();
	}
	
	public AndOrGraph buildGlobalGraph() throws IOException, ClassNotFoundException {
		
		AndOrGraph globalGraph = new AndOrGraph();
		
		for (int i = 0; i< graphCollection.size(); i++) {			
			globalGraph = mergeWithCardinality(graphCollection.get(i), globalGraph, i);	
		}
		
		globalGraph.buildLevelMapBeforeShow();
		assignNames(globalGraph);
		
		return globalGraph;
	}
	
	public AndOrGraph buildGlobalGraph(AndOrGraph previousGlobalGraph) throws IOException, ClassNotFoundException {
		
		AndOrGraph globalGraph = new AndOrGraph();
		globalGraph = previousGlobalGraph;

		for (int i = 0; i< graphCollection.size(); i++) {
			
			graphCollection.get(i).buildLevelMapBeforeShow();	
			globalGraph = mergeWithCardinality(graphCollection.get(i), globalGraph, i);	
		}
		
		globalGraph.buildLevelMapBeforeShow();
		assignNames(globalGraph);
		
		return globalGraph;
	}


	
	public static void assignNames(AndOrGraph g) { 
		
	 HashMap<Integer, OperationNode> opNodeMap = new HashMap();
	 HashMap<Integer, EquivalenceNode> eNodeMap = new HashMap();
	 
	 opNodeMap.putAll(g.getOpNodeList());
	 eNodeMap.putAll(g.getENodeList());
	 
	 /*
	  * Add the parents link in the equivalence node -- basically pointer to all the operation node in which they are participating
	  */
	 
	 for (Entry<Integer,OperationNode> entry: opNodeMap.entrySet()) {
		 
		 int id = entry.getKey();
		 OperationNode opNode = entry.getValue();
		 ArrayList<Integer> childrenIdList = new ArrayList(Arrays.asList(opNode.getChildrenList().stream().toArray(Integer[]::new)));
			 
		 
		 if (childrenIdList.size() == 1) {
			 childrenIdList.add(childrenIdList.get(0));
		 }

		 for (Integer childId : childrenIdList) {
			 eNodeMap.get(childId).addParent(id);
		 }
		 
	 }
	 
	 /*
	  * Find the leave nodes and start naming from there
	  */
	 ArrayList<String> processedNodes =new ArrayList();
	 HashSet<Integer> leaveNodesList = new HashSet();
	 ArrayList<Integer> levelOpNodes = new ArrayList();
	 HashSet<Integer> tempSet = new HashSet();	 
	 ArrayList<Integer> firstLevelNodes = new ArrayList();
	 ArrayList<Integer> secondLevelNodes = new ArrayList();

	 leaveNodesList.addAll(g.getLevelENodeMap().get(1));
	 
	 for (Integer leaveNodeId: leaveNodesList) {
		 
		 ArrayList<Integer> parentsIdList = new ArrayList();
		 parentsIdList.addAll(eNodeMap.get(leaveNodeId).getParentList());
		 
		 for (Integer parentId : parentsIdList) {
			
			 processedNodes.add(parentId.toString());
			 LinkedList<Integer> childrenId = new LinkedList();
			 childrenId.addAll(opNodeMap.get(parentId).getChildrenList());
			
			 EAGVertex vertex;
			 
			 if (childrenId.size() == 1) {
				 childrenId.add(childrenId.get(0));
			 }
			
			 if (leaveNodesList.contains(childrenId.get(0)) && leaveNodesList.contains(childrenId.get(1)))
				 levelOpNodes.add(parentId);
			 
			 if (leaveNodeId.equals(childrenId.get(0))) {
				 
				 vertex = opNodeMap.get(parentId).getLeftOperand().get(0);
				 String relation =vertex.getRelation();
				 String str = parentId+" : sub _ "+relation+" _ "+Integer.toString(parentId)+" - obj _ "+relation+" _ "+Integer.toString(parentId)+" _ L";
				 
				 opNodeMap.get(parentId).addLeftNamingEntry(opNodeMap.get(parentId).getLeftOperand().get(0), str);
				 
				 if (firstLevelNodes.contains(parentId)) {
					 
					 int k = firstLevelNodes.indexOf(parentId);
					 firstLevelNodes.remove(k);
					 secondLevelNodes.add(parentId);

				 } else {
					 firstLevelNodes.add(parentId);
				 }

			 } 
			 
			 if (leaveNodeId.equals(childrenId.get(1))) {
		 
				 vertex = opNodeMap.get(parentId).getRightOperand().get(0);
				 String relation =vertex.getRelation();
				 String str = parentId+" : sub _ "+relation+" _ "+Integer.toString(parentId)+" - obj _ "+relation+" _ "+Integer.toString(parentId)+" _ R";
				 opNodeMap.get(parentId).addRightNamingEntry(opNodeMap.get(parentId).getRightOperand().get(0), str);
 
				 ArrayList<String> keyList = new ArrayList();
				 LinkedList<Integer> tempChildrenList = new LinkedList();
				 Integer j = -10;

				 tempChildrenList.addAll(opNodeMap.get(parentId).getChildrenList());
				
				 
				 if(tempChildrenList.size() == 1) {
					 j = tempChildrenList.get(0);
				 }else {
					 j = tempChildrenList.get(1);
				 }
				 
				 for(EAGVertex e: opNodeMap.get(parentId).getRightOperand() ) {
					 keyList.add(e.getId().toString()+"-"+eNodeMap.get(j).getEAGraph().getTripleIdMap().toString());
				 }

				 if (firstLevelNodes.contains(parentId)) {
					 int k = firstLevelNodes.indexOf(parentId);
					 firstLevelNodes.remove(k);					 
					 secondLevelNodes.add(parentId);

				 } else {
					 firstLevelNodes.add(parentId);
				 }

			 }
			 
		}
		 
	 }
	 
	 tempSet.addAll(levelOpNodes);
	 levelOpNodes.clear();
	 levelOpNodes.addAll(tempSet);

	 /*
	  * Start propagating names in bottom up manner
	  */

	 while (!secondLevelNodes.isEmpty()) {
		 
		 int index = secondLevelNodes.size()-1;
		 int opNodeId = secondLevelNodes.get(index);
	    
		 Integer[] idList = opNodeMap.get(opNodeId).getParentList().stream().toArray(Integer[]::new);
		 int parentENodeId = idList[0];
		
		 processedNodes.add(Integer.toString(opNodeId)+"-"+parentENodeId);

	
	 	if (eNodeMap.get(parentENodeId).getParentCount() > 0 ) {
			 		 
			 for (Integer predecessorId : eNodeMap.get(parentENodeId).getParentList()) {
				 
				 EquivalenceNode eNode = eNodeMap.get(parentENodeId);
				 OperationNode opNode = opNodeMap.get(opNodeId);
				 String side = new String();	 
				 LinkedList<Integer> cList = new LinkedList();

				 cList.addAll(opNodeMap.get(predecessorId).getChildrenList());
				 
				 if (cList.size() == 1) {
					 cList.add(cList.get(0));
				 }
				 
				 if (cList.size() == 2) {
					
					 if (cList.get(0).equals(parentENodeId)) {
						 side = "left";
					 } else {
						 side = "right";
					 }
				} else {
					 System.out.println("/n/n Prolem!!/n/n");
					 System.exit(0);
				 }

				 
				 ArrayList<EAGVertex> rightParticipatingVertex = new ArrayList();
				 ArrayList<EAGVertex> leftParticipatingVertex = new ArrayList();

				 leftParticipatingVertex.addAll(opNode.getLeftVertexMap().keySet());
				 rightParticipatingVertex.addAll(opNode.getRightVertexMap().keySet());
							
				 for (EAGVertex vertex: leftParticipatingVertex) {
				
					 EAGVertex childVertex = null;
					 
					 if (opNode.getLeftVertexMap().containsKey(vertex)) {
						 childVertex = opNode.getLeftNewVertex(vertex);
					 } else {
						 System.out.println("Erro in participating EAGS");
						 System.exit(0);
					 }
					 
					 
					 if (side.equals("left")) {

						 opNodeMap.get(predecessorId).addLeftNamingEntry(vertex, opNode.getNameOfEAG(childVertex));
					 } else {

						 opNodeMap.get(predecessorId).addRightNamingEntry(vertex, opNode.getNameOfEAG(childVertex));
					 }
				 }
				
				 for (EAGVertex vertex: rightParticipatingVertex) {
					 EAGVertex childVertex;
					 
					 if (opNode.getRightVertexMap().containsKey(vertex)) {
						 childVertex = opNode.getRightNewVertex(vertex);
						 
						 if (side.equals("left")) {

								 opNodeMap.get(predecessorId).addLeftNamingEntry(vertex, opNode.getNameOfEAG(childVertex));
							 } else {

								 opNodeMap.get(predecessorId).addRightNamingEntry(vertex, opNode.getNameOfEAG(childVertex));
							 }
							 
					 } else {
						 System.out.println("Erro in participating EAGS");
						 System.exit(0);
					 }
					 
				 }
				
				 				 

				 if (firstLevelNodes.contains(predecessorId)) {
					 
					 int k = firstLevelNodes.indexOf(predecessorId);
					 firstLevelNodes.remove(k);
					 
					 secondLevelNodes.add(predecessorId);
				 } else {
					 firstLevelNodes.add(predecessorId);
				 }
			 }
			 
		 }
		 
		 secondLevelNodes.remove(index);
	 }
	 

	}
			

	
	public AndOrGraph mergeWithCardinality(AndOrGraph g, AndOrGraph global, Integer qId) throws ClassNotFoundException, IOException {
		
	AndOrGraph resultantGraph = new AndOrGraph();
	ArrayList<Integer> eNodeToExplore = new ArrayList();	//Why not hashset
	AndOrGraph gCopy = new AndOrGraph(g);
	boolean print = false;
			
	 /*
	  * Add the parents link in the equivalence node -- basically pointer to all the operation node in which they are participating
	  */
		 
	 for (Entry<Integer,OperationNode> entry: gCopy.getOpNodeList().entrySet()) {
			 
		 int id = entry.getKey();
		 OperationNode opNode = entry.getValue();
		 ArrayList<Integer> childrenIdList = new ArrayList(Arrays.asList(opNode.getChildrenList().stream().toArray(Integer[]::new)));
			 
		if (childrenIdList.size() == 1) {
			childrenIdList.add(childrenIdList.get(0));
		}
			 
		for (Integer childId : childrenIdList) {
			 
			gCopy.getENodeList().get(childId).addParent(id);
		}
			 
	}

	/*
	 * Remove extra opNOde for each level 2 node to avoid stray opNode 
	 */
	
		 HashSet<Integer> levelTwoNodes = g.getLevelENodeMap().get(2);
	
		 for (Integer i: levelTwoNodes) {
			 
			 EquivalenceNode e = g.getENodeList().get(i); 
			 EquivalenceNode eCopy = gCopy.getENodeList().get(i);		 
			 Integer parentOp = e.getChildrenList().iterator().next();
			 Integer copyParentOp = eCopy.getChildrenList().iterator().next();
			 
			 e.clearChildrenList();
			 e.addChild(parentOp);
			 
			 eCopy.clearChildrenList();
			 eCopy.addChild(copyParentOp);
			 
			 g.addENode(e, i);
			 gCopy.addENode(eCopy, i);
		 }
		 
		
		/*
		 * Find the root node of  the graph to be merged and
		 *  check if this node is equivalent to any of the node of global graph 
		 */
		int sizeOfQuery = g.getSize();
		
		if (g.getLevelENodeMap().get(sizeOfQuery).size()>1) {
			
			System.out.println(sizeOfQuery+" ERROR: graph is not in correct format -- too many root nodes \t"+g.getLevelENodeMap().get(sizeOfQuery).size()+"\t"+g.getLevelENodeMap().get(sizeOfQuery).toString());
			System.exit(0);
		} else {
			
			Iterator<Integer> itr = g.getLevelENodeMap().get(sizeOfQuery).iterator();
			int rootId = itr.next();
			int ifEqual = -1;
			
			if (global.getENodeList().size()>0)
			 ifEqual = isEquivalent(global, g.getENodeList().get(rootId));
			
			if (ifEqual != -1) {
				
				if(global.getENodeList().get(ifEqual).isRoot()) {
					// add the naming along with the query id
					global.getENodeList().get(ifEqual).addQueryIds(g.getENodeList().get(rootId).getQueryIdList());
					global.getENodeList().get(ifEqual).putQVarMap(g.getENodeList().get(rootId).getQVarMap());
					global.getENodeList().get(ifEqual).setProjection(g.getENodeList().get(rootId).getProjection());	
				} else {
					
					global.getENodeList().get(ifEqual).setRoot();
					global.getENodeList().get(ifEqual).addQueryIds(g.getENodeList().get(rootId).getQueryIdList());
					global.getENodeList().get(ifEqual).putQVarMap(g.getENodeList().get(rootId).getQVarMap());
					global.getENodeList().get(ifEqual).setProjection(g.getENodeList().get(rootId).getProjection());
				}
				
				queryRootMap.put(qId, ifEqual);
				return global;
				
			} else if (g.getSize() == 2){
			
				queryRootMap.put(qId, rootId);
				eNodeToExplore.addAll(g.getLevelENodeMap().get(2));	
				EquivalenceNode rootENode = g.getENodeList().get(eNodeToExplore.get(0));
				
				if (rootENode.getId().equals(922))
					System.out.println("1. Node 922 is begin touched!!");

				
				Iterator<Integer> it = g.getENodeList().get(rootId).getChildrenList().iterator();

				if (g.getENodeList().get(rootId).getChildrenCount() > 1) {
						
					System.out.println("Error: too many children of base op Node: ");
					System.exit(0);
				
				} else {
					Integer parentOpId = it.next();
					OperationNode opNode = g.getOpNodeList().get(parentOpId);
					Iterator<Integer> itr1 = opNode.getChildrenList().iterator();
					
					if(parentOpId.equals(0)) {
						System.out.print("Opnode wrongly present!! for root "+rootId);
						System.exit(0);
					}				
					
					Integer leftChildId = -1;
					Integer rightChildId = -1;
					boolean singleChild = false;
					boolean leftMatchFound = false;
					boolean rightMatchFound = false;
					
					if (opNode.getChildrenCount() < 2) {
						
						if (itr1.hasNext()) {
							leftChildId = itr1.next();
							
							/*
							 * check equivalence of the leave node
							 */
							
							int matchingLeave = isEquivalent(global, g.getENodeList().get(leftChildId));
							
							if (matchingLeave != -1) {
								leftChildId = matchingLeave;
								leftMatchFound = true;
							} 
							
							rightChildId = leftChildId;
							singleChild = true;
						} else {
							System.out.println("Error: OpNode has no children");
							System.exit(0);
						}
					} else {
						
						if (itr1.hasNext()) {
							leftChildId = itr1.next();
							rightChildId = itr1.next();
							
							int leftMatchingLeave = isEquivalent(global, g.getENodeList().get(leftChildId));
							
							if (leftMatchingLeave != -1) {
								leftChildId = leftMatchingLeave;
								leftMatchFound = true;
							}

							int rightMatchingLeave = isEquivalent(global, g.getENodeList().get(rightChildId));
							
							if (rightMatchingLeave != -1) {
								rightChildId = rightMatchingLeave;
								rightMatchFound = true;
							}
							
						}  else {
							System.out.println("Error: OpNode has no children");
							System.exit(0);
						}
					}
					
					/*
					 * Here, the case of joining same triple is playing a role
					 * as the baseNodes are equivalent to the same nodes of the existing global graph
					 * Therefore opNOde has only one child
					 */
					opNode.clearChildrenList();
					opNode.addChild(leftChildId);
					opNode.addChild(rightChildId);
					rootENode.clearChildrenList();
					rootENode.addChild(opNode.getId());
					global.addOpNode(opNode, parentOpId);
					global.addENode(rootENode, rootId);

					
					/*
					 *  Add non-matched eNode into the global tree
					 */
					
					if (singleChild) {
						
						if (!leftMatchFound) {
				
							EquivalenceNode leftChild = g.getENodeList().get(leftChildId);
							global.addENode(leftChild, leftChildId);
						} 
					} else {
						
						if (!leftMatchFound) {

							EquivalenceNode leftChild = g.getENodeList().get(leftChildId);
							global.addENode(leftChild, leftChildId);
						} 

						if (!rightMatchFound) {
							EquivalenceNode rightChild = g.getENodeList().get(rightChildId);
							global.addENode(rightChild, rightChildId);
						} 
					}				
				} 

			} else {	
				
				queryRootMap.put(qId, rootId);
				HashSet<Integer> leaveNodes = g.getLevelENodeMap().get(1);
				
				/*
				 * Add all the Enode at level two of the tree to eNodeToExplore
				 */
				
				eNodeToExplore.addAll(g.getLevelENodeMap().get(2));
				
				/*
				 * For each ENode (at level2) estimate their cardinality and create a map: estimatedValue -> set<ENodeIDs>
				 */
				
				HashMap<Float,Set<Integer>> cardToENodeMap = new HashMap();
				LinkedList<String> selectedNodes = new LinkedList();
				
				for(Integer eid: eNodeToExplore) {
					
					EquivalenceNode eNode = g.getENodeList().get(eid);
					Float estimatedVal = new Float(0.0); 
					
					// Estimator
					
					CardinalityEstimator estimator = new CardinalityEstimator();
					estimatedVal = estimator.get_estimation(eNode.getAbstractGraph().getTripleList());
					
					if (cardToENodeMap.containsKey(estimatedVal)) {
						cardToENodeMap.get(estimatedVal).add(eid);
						
					} else {
						HashSet<Integer> temp = new HashSet();
						temp.add(eid);
						cardToENodeMap.put(estimatedVal, temp);
					}
				}
				
				/*
				 * Sort the estimated cardinality in increasing order
				 */
				
				ArrayList<Float> sortedEstimatedVal = new ArrayList();
				sortedEstimatedVal.addAll(cardToENodeMap.keySet());
				sortedEstimatedVal.sort(null);
				
				HashSet<Integer> selectedENode = new HashSet(cardToENodeMap.get(sortedEstimatedVal.get(0)));
				
				eNodeToExplore.clear();
				
				/* 
				 * Check if eNodeToExplore is of size >1, if it is then choose only one ENode out of those
				 * Use some tie-breaker to choose the one
				 */
				
				Iterator iterate = selectedENode.iterator();
				eNodeToExplore.add((Integer) iterate.next());

				/*
				 * Start traversing the tree using bottom-up approach
				 * TO-DO: Setup backward links (as done in assign name function) to traverse from bottom to the root (done)
				 */
				
				
				while (!eNodeToExplore.isEmpty()) {
					
					EquivalenceNode currentENode = gCopy.getENodeList().get(eNodeToExplore.get(0));
					HashMap<Integer, Integer> eNodeToOpNode = new HashMap(); //To fetch the corresponding opNode of the selected Nodes
					
					if (currentENode.isRoot())
						break;
					
					HashSet<Integer> parentOpNodeId = new HashSet(currentENode.getParentList());
					HashSet<Integer> parentENodeId = new HashSet(); 
					HashMap<Float, Set<Integer>> estimatedValToEIdMap = new HashMap();
					
					for (Integer opId: parentOpNodeId) {
						
						parentENodeId.addAll(g.getOpNodeList().get(opId).getParentList());
						eNodeToOpNode.put(g.getOpNodeList().get(opId).getParentList().iterator().next(), opId);
					}
					
	
					for (Integer parentEId: parentENodeId) {
						
						EquivalenceNode parentENode = g.getENodeList().get(parentEId);
						Float estimatedVal = new Float(0.0); // TO-Do add estimator function
					
						CardinalityEstimator estimator = new CardinalityEstimator();
						estimatedVal = estimator.get_estimation(parentENode.getAbstractGraph().getTripleList());
						
						
						if (estimatedValToEIdMap.containsKey(estimatedVal)) {
							
							estimatedValToEIdMap.get(estimatedVal).add(parentEId);
							
						} else {
							
							Set<Integer> temp = new HashSet();
							temp.add(parentEId);
							estimatedValToEIdMap.put(estimatedVal, temp);
						}
							
						estimatedValToEIdMap.put(estimatedVal, parentENodeId);
					}
					
					ArrayList<Float> sortedEstimatedValue = new ArrayList();
					sortedEstimatedValue.addAll(estimatedValToEIdMap.keySet());
					sortedEstimatedValue.sort(null);
					Float leastVal = sortedEstimatedValue.get(0);
					HashSet<Integer> selectedParentENode = new HashSet();
					
					/* 
					 * Check if selectedParentENode is of size >1, if it is then choose only one ENode out of those
					 * Use some tie-breaker to choose the one
					 */
					
					selectedParentENode.add(estimatedValToEIdMap.get(leastVal).iterator().next());


					/*
					 * Maintaing list of ENode-ParentOpNode which leads to the next ENOde in this list
					 * later will check equivalence in the global tree and merge these nodes and opNodes
					 */
					
					Integer selectedENodeId = selectedParentENode.iterator().next();
					Integer correspondingOpNode = eNodeToOpNode.get(selectedENodeId);
					selectedNodes.add(currentENode.getId()+"-"+correspondingOpNode);

					/*
					 * Add the selected parentENode to the to be explored list
					 * First empty the list and then add
					 */

					eNodeToExplore.clear();
					eNodeToExplore.add(selectedENodeId);
					
					/*
					 * To remove the non-selected opNodes of the selectedENode
					 */
					EquivalenceNode e = g.getENodeList().get(selectedENodeId);
					e.clearChildrenList();
					e.addChild(correspondingOpNode);
					g.addENode(e, selectedENodeId);
					EquivalenceNode copyE = gCopy.getENodeList().get(selectedENodeId);
					copyE.clearChildrenList();
					copyE.addChild(correspondingOpNode);
					gCopy.addENode(copyE, selectedENodeId);
				}
				
				
				boolean matchingENodeFound = false;
				String firstEle = selectedNodes.getFirst();
				String lastEle = selectedNodes.getLast();
				LinkedList<String> copySelectedNodes = new LinkedList();
				copySelectedNodes.addAll(selectedNodes);
				
				/*
				 * Merge the ENodes of the selectedNodes list to the global tree 
				 */
				
				
				while (selectedNodes.size()>0) {
					
					String str = selectedNodes.getLast();
					String[] strComp = str.split("-"); //strComp[0] = enode id, strComp[1] = chosen parent op node id
					Integer parentOpNodeId = Integer.parseInt(strComp[1]);
					Integer eNodeId = Integer.parseInt(strComp[0]);
					Integer matching = isEquivalent(global, g.getENodeList().get(eNodeId));
					OperationNode parentOpNode = g.getOpNodeList().get(parentOpNodeId);
					Iterator<Integer> it = parentOpNode.getChildrenList().iterator();
					Integer leftChildId = -1;
					Integer rightChildId = -1;
					boolean singleChild = false;
									
					
					if (parentOpNode.getChildrenCount() < 2) {
						
						if (it.hasNext()) {
							leftChildId = it.next();
							rightChildId = leftChildId;
							singleChild = true;
						} else {
							System.out.println("Error: OpNode has no children");
							System.exit(0);
						}
					} else {
						
						if (it.hasNext()) {
							leftChildId = it.next();
							rightChildId = it.next();
						}  else {
							System.out.println("Error: OpNode has no children");
							System.exit(0);
						}
					}

					
					if(matching != -1) {
						
						matchingENodeFound = true;
						
						if (leftChildId.equals(eNodeId)) {
							
							parentOpNode.clearChildrenList();
							parentOpNode.addChild(matching);
							
							if (singleChild)
								parentOpNode.addChild(matching);
							else {

								Integer rightMatch = isEquivalent(global, g.getENodeList().get(rightChildId));
								
								if (rightMatch != -1) {
									parentOpNode.addChild(rightMatch);
									
								} else {
									
									if (leaveNodes.contains(rightChildId)) {
										parentOpNode.addChild(rightChildId);
										global.addENode(g.getENodeList().get(rightChildId), rightChildId);
									
									} else {
										
										System.out.println("Error: Right operand is not a leaf node");
										System.exit(0);
									}
								}
							}
						
						} else if (rightChildId.equals(eNodeId)) {
							
							parentOpNode.clearChildrenList();
							
							if (singleChild) {
								parentOpNode.addChild(matching);
								parentOpNode.addChild(matching);
								
							} else {
								
								Integer leftMatch = isEquivalent(global, g.getENodeList().get(leftChildId));
								
								if (leftMatch != -1) {
									parentOpNode.addChild(leftMatch);
								
								} else {
									
									if (leaveNodes.contains(leftChildId)) {
										parentOpNode.addChild(leftChildId);
										global.addENode(g.getENodeList().get(leftChildId), leftChildId);
										
									} else {
										System.out.println("Error: Left operand is not a leaf node");
										System.exit(0);
									}	
								}
								
								parentOpNode.addChild(matching);
							}							
						} else {
							System.out.println("Some error in matching!!");
							System.exit(0);
						}
						
					} else {
						
						/*
						 * Remove the last element of selectedNodes linked list
						 * Add the eNode to the global tree as no matching of it has been found
						 */
						
						/*
						 * Check the other children of parent op node
						 */
						
						if (leftChildId.equals(eNodeId) && (!singleChild)) {
							
							parentOpNode.clearChildrenList();
							parentOpNode.addChild(leftChildId);
							
							Integer rightMatch = isEquivalent(global, g.getENodeList().get(rightChildId));
								
								if (rightMatch != -1) {
									parentOpNode.addChild(rightMatch);
	
								} else {
									EquivalenceNode eRight = g.getENodeList().get(rightChildId);
									
		
									if(leaveNodes.contains(rightChildId)) {
									
										global.addENode(eRight, rightChildId); 
										parentOpNode.addChild(rightChildId);							
									} else {			
											System.out.println("Error: Non-matchin code - Right operand is not leave!!");
											System.exit(0);
									}
								}
													
						} else if (rightChildId.equals(eNodeId) && (!singleChild)) {
							
							parentOpNode.clearChildrenList();
													
							Integer leftMatch = isEquivalent(global, g.getENodeList().get(leftChildId));
								
							if (leftMatch != -1) {
								parentOpNode.addChild(leftMatch);
								
							} else {
								
								EquivalenceNode eLeft = g.getENodeList().get(leftChildId);
								
								if (leaveNodes.contains(leftChildId)) {
									parentOpNode.addChild(leftChildId);									
									global.addENode(eLeft, leftChildId);
						
								}  else {
									
									System.out.println("Error: Non-matchin code - Left operand is not leave!!");
									System.exit(0);
								}

							}
							
							parentOpNode.addChild(rightChildId);
						}
							
						EquivalenceNode e = g.getENodeList().get(eNodeId);	
						global.addENode(e, eNodeId);
					}
					
					global.addOpNode(parentOpNode, parentOpNodeId);				
					selectedNodes.removeLast();

					if (matchingENodeFound)
						break;
				}
				
				
				/*
				 * Check equivalence for the leave nodes of the "first"  opNodes
				 */
				
				String sComp[] = firstEle.split("-");
				Integer firstENode = Integer.parseInt(sComp[0]);
				Integer baseNodeMatch = isEquivalent(global, g.getENodeList().get(firstENode));
				
				  if (!matchingENodeFound) {	
					
					  if (g.getENodeList().keySet().contains(baseNodeMatch)) {
					
						  Iterator<Integer> it = g.getENodeList().get(firstENode).getChildrenList().iterator();

					if (g.getENodeList().get(firstENode).getChildrenCount() > 2) {
						Integer fChild = it.next();
						Integer sChild = it.next();
						
						System.out.println("Error: too many children of base op Node: "+g.getOpNodeList().get(fChild).getChildrenList().toString()+"\t"+
						g.getOpNodeList().get(sChild).getChildrenList().toString());
						System.exit(0);
					
					} else {
						Integer parentOpId = it.next();
						OperationNode opNode = g.getOpNodeList().get(parentOpId);

						Iterator<Integer> itr1 = opNode.getChildrenList().iterator();
						
						
						Integer leftChildId = -1;
						Integer rightChildId = -1;
						boolean singleChild = false;
						boolean leftMatchFound = false;
						boolean rightMatchFound = false;
						
						if (opNode.getChildrenCount() < 2) {
							
							if (itr1.hasNext()) {
								leftChildId = itr1.next();
								
								/*
								 * check equivalence of the leave node
								 */
								
								int matchingLeave = isEquivalent(global, g.getENodeList().get(leftChildId));
								
								if (matchingLeave != -1) {
									leftChildId = matchingLeave;
									leftMatchFound = true;
								}
										
								rightChildId = leftChildId;
								singleChild = true;
							
							} else {
								System.out.println("Error: OpNode has no children");
								System.exit(0);
							}
						} else {
							
							if (itr1.hasNext()) {
								leftChildId = itr1.next();
								rightChildId = itr1.next();
								
								int leftMatchingLeave = isEquivalent(global, g.getENodeList().get(leftChildId));
								
								if (leftMatchingLeave != -1) {
									leftChildId = leftMatchingLeave;
									leftMatchFound = true;
								}

								int rightMatchingLeave = isEquivalent(global, g.getENodeList().get(rightChildId));
								
								if (rightMatchingLeave != -1) {
									rightChildId = rightMatchingLeave;
									rightMatchFound = true;

								}
								
							}  else {
								System.out.println("Error: OpNode has no children");
								System.exit(0);
							}
						}
						
						
						opNode.clearChildrenList();
						opNode.addChild(leftChildId);
						opNode.addChild(rightChildId);
						
						global.addOpNode(opNode, parentOpId);
						EquivalenceNode baseENode;
						baseENode = global.getENodeList().get(firstENode);
							
						baseENode.clearChildrenList();
						baseENode.addChild(parentOpId);
						global.addENode(baseENode, firstENode);

							
						/*
						 *  Add non-matched eNode into the global tree
						 */
							
						if (singleChild) {
								
							if (!leftMatchFound) {
									
								EquivalenceNode eLeft = g.getENodeList().get(leftChildId);	
								global.addENode(eLeft, leftChildId);		
							}
						} else {
								
							if (!leftMatchFound) {
									
								EquivalenceNode eLeft = g.getENodeList().get(leftChildId);
								global.addENode(eLeft, leftChildId);		
							}

							if (!rightMatchFound) {
							
								EquivalenceNode eRight = g.getENodeList().get(rightChildId);			
								global.addENode(eRight, rightChildId);
							}
						}				
					}

				}
			} // Checking wait
				
			/*
			 * Add the root node of g as well
			 */
				
			String strComp[] = lastEle.split("-");
			Integer opId = Integer.parseInt(strComp[1]);
			EquivalenceNode root = g.getENodeList().get(rootId);
				
			root.clearChildrenList();
			root.addChild(opId);
			global.addENode(root, rootId);		
		}
	}	
		
	
		int gSize = g.getSize();
		int globalSize = global.getSize();
			
		if (globalSize > gSize) {
				
			resultantGraph.setSize(globalSize);
		} else {
			
			resultantGraph.setSize(gSize);
			global.setSize(gSize);
		}
			
		return global;
	}

	public Integer isEquivalent(AndOrGraph g, EquivalenceNode e) {
		
		for (Entry<Integer, EquivalenceNode> entry: g.getENodeList().entrySet()) {
			
			if (entry.getValue().areEqual(e)) {
				return entry.getKey();
			}
		}
		
		return -1;
	}
	

}
