package preprocessing.queryRegistry.executionPlan.local;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

/**
 * ExtendedAbstractGraph is the abstractForm of the Query Graph(Abstract Graph) where each node
 * represents the abstract form of a triple
 */
public class ExtendedAbstractGraph implements java.io.Serializable {
	private Integer Id;
	private HashMap<Integer, EAGVertex> tripleIdMap;	
	private ArrayList<String> tripleList;	//abstractform of EAGVertexs
	private HashMap<Integer, HashSet<String>> graph;		// 1 -> {2:SS, 4:OS}
	private HashMap<Integer, HashSet<Integer>> neighborMap;	//1 -> {2,4}
	
	public ExtendedAbstractGraph(Integer id, AbstractGraph aGraph) {
		this.Id = id;
		int i = 0;
		this.tripleList = new ArrayList();
		this.tripleIdMap = new HashMap();
		
		/*
		 * Put the abstract form of each triple(node) of the abstractGraph which
		 * is passed as a parameter
		 */
		
		for (TriplePattern pattern: aGraph.getTripleList()) {
			i = pattern.getId();
			EAGVertex vertex = new EAGVertex(i, pattern.getAbstractForm());
			tripleList.add(pattern.getAbstractForm());
			tripleIdMap.put(i, vertex);
		}
		
		this.graph = new HashMap();
		this.graph.putAll(aGraph.getAbstractGraph());
		this.neighborMap = new HashMap();
		this.neighborMap.putAll(aGraph.getNeighbors());
		HashMap<Integer, EAGVertex> tempMap = new HashMap();
		tempMap.putAll(tripleIdMap);
		
		for (Entry<Integer, EAGVertex> entry: tempMap.entrySet()) {
			ArrayList<String> neighbors = new ArrayList();
			
			//*********Adding to fix the naming conventon problem *****************
			
			HashSet<String> str = graph.get(entry.getKey());
			HashMap<Integer, String> temp = new HashMap();
			
			for (String s: str) {
				
				String sComp[] = s.split(" : ");
				temp.put(Integer.parseInt(sComp[0]), sComp[1]);
			}
			
			//****************************************************************
			for (Integer index: neighborMap.get(entry.getKey())) {
	//			neighbors.add(tripleIdMap.get(index).getForm());		
				
				neighbors.add(tripleIdMap.get(index).getForm()+" : "+temp.get(index));				

			}
			//System.out.println("Checking:"+id+": "+entry.getKey()+"\t"+neighbors.toString());
			//tripleIdMap.get(entry.getKey()).setNeighbors(neighbors);
			entry.getValue().setNeighbors(neighbors);
		//	System.out.println("Checking:"+id+": "+entry.getKey()+"\t"+neighbors.toString());
			tripleIdMap.put(entry.getKey(), entry.getValue());
		}
	}

	public HashMap<Integer, EAGVertex> getTripleIdMap() {
		return this.tripleIdMap;
	}
	
	public  HashMap<Integer, HashSet<String>> getGraph() {
		return this.graph;
	}
	
	public  HashMap<Integer, HashSet<Integer>> getNeighborMap() {
		return this.neighborMap;
	}
}
