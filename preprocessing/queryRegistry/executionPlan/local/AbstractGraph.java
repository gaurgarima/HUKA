package preprocessing.queryRegistry.executionPlan.local;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

/**
 * This is the graphical representation of the query where each node represents a query triple
 * and an edge between two node represents that two corresponding triples can be joined and edge is labelled
 *  with the type of join -- SS, OS, SO, OO 
 *  @author garima
 *
 */

public class AbstractGraph implements java.io.Serializable {
	private Integer Id;
	private HashMap<Integer, String> tripleIdMap;	//1 -> A, rdf:type, B
	private ArrayList<TriplePattern> tripleList;
	private HashMap<Integer, HashSet<String>> graph;		// 1 -> {2:SS, 4:OS}
	private HashMap<Integer, HashSet<Integer>> neighborMap;	//1 -> {2,4}
	private String graphType;


	
	public AbstractGraph(Integer id, HashMap<Integer, String> map, boolean buildGraph) {
		
		this.Id =id;
		this.tripleIdMap = new HashMap();
		this.tripleIdMap.putAll(map);
		String tripleStr;
		tripleList =new ArrayList();
		
		/*
		 * Constructing the nodes of the graph
		 */
		for (Integer i: tripleIdMap.keySet()) {
			tripleStr = tripleIdMap.get(i);	
			String[] tripleComp = tripleStr.split(" "); 
			TriplePattern triple = new TriplePattern(i, tripleComp[0], tripleComp[1], tripleComp[2]);
			tripleList.add(triple);
		}
		

		/*
		 * Decide if the abstract graph is representing a leaf node or internal node 
		 */
		if (tripleList.size() == 1) {
			graphType = "leave";
		} else {
			graphType = "internal node";
		}
		
		/*
		 *  If the buildGraph is set (true) then call buildAbstractGraph() method
		 */
		if (buildGraph) {
			buildAbstractGraph();
		}
		
	}

	
	/**
	 *  This method generates the abstract graph and stor them in graph field as shown in the dummy example
	 */

	void buildAbstractGraph() {
		
		graph = new HashMap();
		neighborMap = new HashMap();
		
		for (int i = 0 ; i <tripleList.size(); i++) {
			TriplePattern triple1 = tripleList.get(i);
			String joinType = null;
			HashSet<String> edgeList =new HashSet();
			HashSet<Integer> neighborList = new HashSet();
			
			for(int j=0; j < tripleList.size(); j++) {
				
				if (i == j) {
					joinType = "-";
				} else {
					joinType = findJoinType (triple1, tripleList.get(j));
					
				}
				
				if (!(joinType.equals("NULL") || joinType.equals("-"))) {
					edgeList.add(Integer.toString(tripleList.get(j).getId()).concat(" : "+joinType));
					neighborList.add(tripleList.get(j).getId());
				}

				
				}
			
			graph.put(triple1.getId(), edgeList);
			neighborMap.put(triple1.getId(), neighborList);
		}
	}
	
	private String findJoinType(TriplePattern triple1, TriplePattern triple2) {
		String joinType = "NULL";
		
		if (triple1.getSubject().equals(triple2.getSubject())) {
			joinType = "SS";
		} else if (triple1.getObject().equals(triple2.getSubject())) {
			joinType = "OS";
		} else if (triple1.getSubject().equals(triple2.getObject())) {
			joinType = "SO";
		} else if (triple1.getObject().equals(triple2.getObject())) {
			joinType = "OO";
		}
		
		return joinType;
	}
	
	/**
	 *  This method constructs extended abstract graph where each node holds
	 *  the abstract form of the triple and edges are same as in the abstract graph.
	 *  extendedAbstractGraph field holds the graph where first entries represents the 
	 *  node and subsequent items represents its neighbor
	 */
	
	/*
	 	String patternAbstractForm = new String();
		
		for (Entry<Integer, HashSet<String>> entry : graph.entrySet()) {
			TriplePattern pattern = tripleList.get(entry.getKey());
			patternAbstractForm = pattern.getAbstractForm();
			ArrayList<String> graphEntry = new ArrayList();
			graphEntry.add(patternAbstractForm);
			String neighborAbstractForm =new String();
			String newNeighbor = new String();
			
			for (String neighbor: entry.getValue()) {
				String[] neighborComp = neighbor.split(":");
				neighborAbstractForm = tripleList.get(Integer.parseInt(neighborComp[0])).getAbstractForm();
				newNeighbor = neighborAbstractForm.concat(":").concat(neighborComp[1]);
				graphEntry.add(newNeighbor);
			}
			extendedAbstractGraph.add(graphEntry);
		}
	}
	*/
	
	/*
	 *  Get functions
	 */
	public HashMap<Integer, String> getTripleIdMap() {
		return this.tripleIdMap;
	}
	
	public Integer getId() {
		return Id;
	}
	
	public ArrayList<TriplePattern> getTripleList() {
		return this.tripleList;
	}
	
	public HashMap<Integer, HashSet<String>> getAbstractGraph() {
		return this.graph;
	}

	public String getGraphType() {
		return this.graphType;
	}

	/*
	public ArrayList<ArrayList<String>> getExtendedAbstractGraph() {
		return this.extendedAbstractGraph;
	}
	*/
	
	public HashMap<Integer, HashSet<Integer>> getNeighbors() {
		return this.neighborMap;
	}
	
	/*
	 * Set graphType
	 */
	public void setGraphType(String type) {
		this.graphType = type;
	}
}
