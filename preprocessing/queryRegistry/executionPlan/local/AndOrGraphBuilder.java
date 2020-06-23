package preprocessing.queryRegistry.executionPlan.local;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.jena.ext.com.google.common.collect.Sets;
import org.apache.jena.query.Query;


/**
 * This class takes a SPARQL query as input and build AND-Or graph for it
 * @author garima
 *
 */
public class AndOrGraphBuilder {
	private HashMap<Integer, String> queryTripleMap;
	private Integer equivNodeId;
	private Integer opNodeId;
	private AndOrGraph graph;
	private EquivalenceNode rootNode;
	
	public AndOrGraphBuilder(Integer equivNodeId, Integer opNodeId, HashMap<Integer, String> map, HashSet<String> toProject, String qId) {
		
		this.queryTripleMap = new HashMap();
		this.queryTripleMap.putAll(map);
		this.equivNodeId = new Integer(equivNodeId);
		this.opNodeId = new Integer(opNodeId);
	    this.rootNode = new EquivalenceNode(equivNodeId, queryTripleMap, true, qId);
	    this.equivNodeId++;
	    HashMap<String, HashSet<String>> tempMap = new HashMap();
	    tempMap.put(qId, toProject);
	    this.rootNode.setProjection(tempMap);

	}
	
	public AndOrGraph buildGraph() {
				
		int startENodeId = equivNodeId;
		int startOpNodeId = opNodeId;
		HashMap<Integer, Integer> baseRelationENodeMap= new HashMap(); // to store the triple id -> endoe id  (for the base relation/ triple of the query
		HashMap<Integer, OperationNode> opNodeMap = new HashMap();
		HashMap<Integer, EquivalenceNode> equivNodeMap = new HashMap();
		HashMap<HashSet<Integer>, Integer> tripleToENodeIdMap = new HashMap();	//{0,2,1} -> 4, where {0,1,2} are triple ids which constitute enode with id 4
		
		/*
		 * Build leave nodes of the graph, corresponding to each triple of the query
		 */
		for (Entry<Integer, String> entry: queryTripleMap.entrySet()) {
			
			/*
			 * Create equivNode for each triple
			 */
			HashMap<Integer, String> map = new HashMap();
			map.put(entry.getKey(), entry.getValue());
		
			EquivalenceNode eNode = new EquivalenceNode(equivNodeId, map, false, null);
			equivNodeMap.put(equivNodeId, eNode);
			baseRelationENodeMap.put(entry.getKey(), eNode.getId());
			equivNodeId++;
			HashSet<Integer> set = new HashSet();
			set.add(entry.getKey());
						
			tripleToENodeIdMap.put(set, eNode.getId());
		}
		
		/*
		 * Add neighbors -- translating triple neighbors to EnOde neighbors
		 */
		for (Entry<Integer, EquivalenceNode> entry: equivNodeMap.entrySet()) {
			
			EquivalenceNode eNode = entry.getValue();
			AbstractGraph aGraph = eNode.getAbstractGraph();
			HashMap<Integer, HashSet<String>> graph = aGraph.getAbstractGraph();	//TO-DO: check the value of this graph map: this should look like: i -> i:-
			HashSet<String> joinCompatible = new HashSet<String>();
			
			for (Integer index: graph.keySet()) {
				HashSet<String> neighborsList = new HashSet<String>();
				neighborsList.addAll(rootNode.getAbstractGraph().getAbstractGraph().get(index));	//TO:DO: two consecutive call to differnet function but with same name looks funny; change it
				
				for (String neighbor : neighborsList) {

					String[] neighborComp = neighbor.split(" : ");
					String jType = neighborComp[1];
					Integer neighborTripleId = Integer.parseInt(neighborComp[0]);
					HashSet<Integer> set = new HashSet<Integer>();
					set.add(neighborTripleId);
					Integer neighborENodeId = tripleToENodeIdMap.get(set);
					String joinType = neighborENodeId.toString().concat(" : ")+index+" : "+jType+" : "+neighborTripleId;
				
					// joinType = negiborinEnodeId:tripleId of the observation Enode:(SS/SO/OS/OO):neghboring Enode tripleId
					joinCompatible.add(joinType);
				}
			}
			
			
			eNode.setJoinCompatibility(joinCompatible);
			equivNodeMap.put(entry.getKey(), eNode);
		}
		

		/*
		 * Start creating internal nodes of the graph
		 */
		
		HashMap<Integer, EquivalenceNode> currentLevelENode = new HashMap();
		currentLevelENode.putAll(equivNodeMap);
		HashMap<Integer, EquivalenceNode> nextLevelENode = new HashMap();
	
		for (int level = 1; level < rootNode.getLevel(); level++) {
			
			for (Entry<Integer, EquivalenceNode> entry: currentLevelENode.entrySet()) {
			
				EquivalenceNode eNode = entry.getValue();
				HashSet<String> eNodeJoinCompatibleSet = new HashSet();
				eNodeJoinCompatibleSet.addAll(eNode.getJoinCompatibility());
				
				/*
				 * Join the equivalence Nodes which are compatible to eNode
				 */
				
				for (String join : eNodeJoinCompatibleSet) {					
					String[] joinComp = join.split(" : ");
					Integer toJoinNodeId = Integer.parseInt(joinComp[0]);
					String[] eNodeJoiningTripleIdSet = joinComp[1].split("-");
					String joinType = joinComp[2];
					String[] joiningTripleIdSet = joinComp[3].split("-");
					
					/*
					 * Merging all the triple that constitute this newENode (resultant of joining eNode and toJoinENode)
					 */
					EquivalenceNode toJoinNode = equivNodeMap.get(toJoinNodeId);
					EquivalenceNode newENode;
					HashSet<Integer> newENodeTripleSet = new HashSet();
					Set<Integer> eNodeTripleSet = eNode.getAbstractGraph().getTripleIdMap().keySet();
					Set<Integer> toJoinNodeTripleSet =  toJoinNode.getAbstractGraph().getTripleIdMap().keySet();

					newENodeTripleSet.addAll(eNodeTripleSet);
					newENodeTripleSet.addAll(toJoinNodeTripleSet);
					
					/*
					 * construct an operation nodes for this join
					 */ 
					
					OperationNode opNode = new OperationNode(opNodeId, level);
					opNodeId++;
					opNode.setLevel(level);
					opNode.addChild(entry.getKey()); // Add left child
					opNode.addChild(toJoinNodeId); //Add Right child
					opNode.setOperation(joinType);
					
					if (eNodeJoiningTripleIdSet.length != joiningTripleIdSet.length) {
						System.out.println("Something is wrong!!!");
					} else {
						
						for (int k =0; k< joiningTripleIdSet.length ; k++) {
					
							String str = baseRelationENodeMap.get(Integer.parseInt(eNodeJoiningTripleIdSet[k]))+" : "+baseRelationENodeMap.get(Integer.parseInt(joiningTripleIdSet[k]));
							opNode.addOperands(str);
						}
					}
					
					
					for (String str: eNodeJoiningTripleIdSet) {
	
						opNode.addLeftEAVertex(eNode.getEAGraph().getTripleIdMap().get(Integer.parseInt(str)));
					}
					
					for (String str: joiningTripleIdSet) {
						opNode.addRightEAVertex(toJoinNode.getEAGraph().getTripleIdMap().get(Integer.parseInt(str)));
					}

					/*
					 * Check if new node is already present in the graph
					 */
					
					if (tripleToENodeIdMap.containsKey(newENodeTripleSet)) {
						int i = tripleToENodeIdMap.get(newENodeTripleSet);
						newENode = equivNodeMap.get(i);
						newENode.addChild(opNode.getId());
						equivNodeMap.put(i, newENode);  

					} else {
						
						HashMap<Integer, String> tripleIdMap = new HashMap();
						tripleIdMap.putAll(eNode.getAbstractGraph().getTripleIdMap());
						tripleIdMap.putAll(toJoinNode.getAbstractGraph().getTripleIdMap());
					
					
						newENode = new EquivalenceNode(equivNodeId, tripleIdMap, false, null);
						equivNodeId++;
						tripleToENodeIdMap.put(newENodeTripleSet, newENode.getId());
						
				
						HashSet<String> newJoinCompatibility = new HashSet<String>();
						HashSet<String> tempSet = new HashSet();
						tempSet.addAll(eNodeJoinCompatibleSet);
						HashSet<String> tempSet1 =new HashSet();
						HashSet<Integer> commonTripleSet = findIntersection(eNodeTripleSet, toJoinNodeTripleSet);
						
						/*
						 * Finding diff_set = enode tripleset - commonSet
						 */
						HashSet<Integer> eNodeTripleDiffSet = new HashSet();
						eNodeTripleDiffSet.addAll(eNodeTripleSet);
						eNodeTripleDiffSet.removeAll(commonTripleSet);
						
						HashSet<Integer> toJoinNodeTripleDiffSet = new HashSet();
						toJoinNodeTripleDiffSet.addAll(toJoinNodeTripleSet);
						toJoinNodeTripleDiffSet.removeAll(commonTripleSet);
						HashSet<Integer> uncommonSet = new HashSet<Integer>();
						uncommonSet.addAll(eNodeTripleDiffSet);
						uncommonSet.addAll(toJoinNodeTripleDiffSet);
						
						for (String str: eNodeJoinCompatibleSet) {
							
							if (str.startsWith(toJoinNodeId+" : ")) {
								tempSet.remove(str);
							} else {
								tempSet1.add(str.split(" : ")[0]);
							}
							
							if (!str.contains("-")) {
								if (uncommonSet.contains(Integer.parseInt(str.split(" : ")[3]))) {
									tempSet.remove(str);
								}
							}
						}
						
						newJoinCompatibility.addAll(tempSet);
						tempSet.clear();
						tempSet.addAll(toJoinNode.getJoinCompatibility());
						
						for (String str: toJoinNode.getJoinCompatibility()) {
							
							if (str.startsWith(entry.getKey()+" : ") || tempSet1.contains(str.split(" : ")[0])) {
								tempSet.remove(str);
							}
							if (!str.contains("-")) {
								if (uncommonSet.contains(Integer.parseInt(str.split(" : ")[3]))) {
									tempSet.remove(str);
								}
							}
							
						}
						
						newJoinCompatibility.addAll(tempSet);
						
						/*
						 * Now start checking currentLevel list for joins
						 */
				
						newENode.setJoinCompatibility(newJoinCompatibility);
						newENode.addChild(opNode.getId());
						equivNodeMap.put(newENode.getId(), newENode);				

					}
				
					// For naming convention

					for (Integer i: eNodeTripleSet) {
						
						EAGVertex leftOldVertex = eNode.getEAGraph().getTripleIdMap().get(i);
						EAGVertex leftNewVertex = newENode.getEAGraph().getTripleIdMap().get(i);
						opNode.addNewLVertex(leftNewVertex, leftOldVertex);

					}
					
					for (Integer i: toJoinNodeTripleSet) {
							
						EAGVertex rightOldVertex = toJoinNode.getEAGraph().getTripleIdMap().get(i);
						EAGVertex rightNewVertex = newENode.getEAGraph().getTripleIdMap().get(i);
						opNode.addNewRVertex(rightNewVertex, rightOldVertex);
					}
					
					
				 EAGVertex leftEAG = opNode.getLeftOperand().get(0);
				 EAGVertex rightEAG = opNode.getRightOperand().get(0);

				 LinkedHashSet<Integer> l = opNode.getChildrenList();
				 Iterator<Integer> it = l.iterator();
				 Integer leftChild = it.next();
				 Integer rightChild = it.next();
				 
				 EquivalenceNode left = equivNodeMap.get(leftChild);
				 EquivalenceNode right = equivNodeMap.get(rightChild);
				 
					
				opNode.addParent(newENode.getId());
				opNodeMap.put(opNode.getId(), opNode);

				nextLevelENode.put(newENode.getId(), newENode);
			}			
		}
			
			/*
			 * Clear the list and updat current list to the next level E ndoes	
			 */
			currentLevelENode.clear();
			currentLevelENode.putAll(nextLevelENode);
			nextLevelENode.clear();
		}
		
		int endENodeId = equivNodeId - 1;
		int endOpNodeId = opNodeId - 1;
				
		graph = new AndOrGraph(equivNodeMap, opNodeMap, startENodeId, endENodeId, startOpNodeId, endOpNodeId, rootNode.getLevel(), rootNode);


		return graph;
	}

	
	private HashSet<Integer> findIntersection(Set<Integer> eNodeTripleSet, Set<Integer> toJoinNodeTripleSet) {
		HashSet<Integer> commonSet = new HashSet();
		commonSet.addAll(eNodeTripleSet);
		commonSet.retainAll(toJoinNodeTripleSet);
		
		return commonSet;
	}

}
