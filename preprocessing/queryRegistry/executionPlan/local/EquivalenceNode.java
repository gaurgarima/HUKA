package preprocessing.queryRegistry.executionPlan.local;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Equivalence node represents an intermediate expressin in the AND-OR graph
 * which itself is query execution plan
 * @author garima
 *
 */
public class EquivalenceNode extends AndOrGraphNode implements java.io.Serializable {
	
	private AbstractGraph expressionGraph;
	private long estimatedCardinality;
	private HashSet<String> joinCompatibleNodeList;
	private HashMap<Integer,String> attributesList;	//parents dependent attribute list
	private ExtendedAbstractGraph extendedGraph;
	private Integer size;
	private boolean isRootNode;
	private HashMap<String,HashMap<EAGVertex, ArrayList<String>>> qVariableMapping;
	private HashMap<String, HashSet<String>> projectingVariables;
	private ArrayList<String> queryIdList;
	private HashMap<Integer, String> map;
	
	public EquivalenceNode(EquivalenceNode e) {
		this(e.getId(), e.getMap(), e.isRoot(), "0");
	}
	
	
		
	public EquivalenceNode(Integer id, HashMap<Integer, String> map, boolean rootFlag, String queryId) {
		
		super();
		setId(id);
		
		this.map = new HashMap();
		this.map.putAll(map);
		
		setLevel(map.size());
		expressionGraph = new AbstractGraph(id, map,true);
		extendedGraph = new ExtendedAbstractGraph(id, expressionGraph);
		this.size = expressionGraph.getTripleList().size();
		this.isRootNode = rootFlag;

		if (rootFlag) {
			
			this.queryIdList = new ArrayList();
			this.qVariableMapping = new HashMap();
			this.projectingVariables = new HashMap();

			HashMap<Integer, String> aGraphTemp = new HashMap();
			HashMap<Integer, EAGVertex> eaGraphTemp = new HashMap();
			
			aGraphTemp.putAll(expressionGraph.getTripleIdMap());
			eaGraphTemp.putAll(extendedGraph.getTripleIdMap());
			HashMap<EAGVertex, ArrayList<String>> temp = new HashMap();
			
			for (Integer tripleId: map.keySet()) {
				
				if (temp.containsKey(eaGraphTemp.get(tripleId)))
					temp.get(eaGraphTemp.get(tripleId)).add(aGraphTemp.get(tripleId));
				else {
					ArrayList<String> t = new ArrayList();
					t.add(aGraphTemp.get(tripleId));
					
					temp.put(eaGraphTemp.get(tripleId), t);
							
				}
				
			}
			
			this.queryIdList.add(queryId);
			qVariableMapping.put(queryId, temp);
		}
		
		
	}
	
	
	public HashMap<Integer, String> getMap() {
		return this.map;
	}
	public void setQueryId (String i) {
		
			queryIdList.add(i);
	}
	
	public void addQueryIds (ArrayList<String> list) {
		queryIdList.addAll(list);
	}
	
	public ArrayList<String> getQueryIdList () {
		
		return queryIdList;
	}
	
	public HashMap<String,HashMap<EAGVertex, ArrayList<String>>> getQVarMap() {
		return qVariableMapping;
	}
	
	public void putQVarMap( HashMap<String,HashMap<EAGVertex, ArrayList<String>>> map) {
		
		//this.qVariableMapping = new HashMap();
		qVariableMapping.putAll(map);
	}
	
	public boolean isRoot() {
		return isRootNode;
	}
	
	public void setRoot() {
		this.queryIdList = new ArrayList();
		this.qVariableMapping = new HashMap();
		this.projectingVariables = new HashMap();
		isRootNode = true;
	}
	
	public void setEstimatedCardinality(long card) {
		this.estimatedCardinality = card;
	}
	
	public void setJoinCompatibility(HashSet<String> joinCompatibilityList) {
		
		this.joinCompatibleNodeList = new HashSet<String>();
		this.joinCompatibleNodeList.addAll(joinCompatibilityList);
	}
	
	
	
	public HashSet<String> getJoinCompatibility() {
		return this.joinCompatibleNodeList;
	}
	
	public long getEstiamtedCardinality() {
		return this.estimatedCardinality;
	}

	public AbstractGraph getAbstractGraph() {
		return this.expressionGraph;
	}
	
	public ExtendedAbstractGraph getEAGraph() {
		return this.extendedGraph;
	}
	
	public boolean areEqual(EquivalenceNode obj) {
		
		ArrayList<EAGVertex> set = new ArrayList();
		set.addAll(this.extendedGraph.getTripleIdMap().values());
		ArrayList<EAGVertex> set1 = new ArrayList();
		set1.addAll(obj.getEAGraph().getTripleIdMap().values());
		boolean first = true;
	//	System.out.println("Check:"+set.size()+"\t"+set1.size());
		
		if (set.size() != set1.size()) {
			first = false;
		
		} else {
			for (EAGVertex vertex: set) {
				int flag = 0;
				
				for (EAGVertex vertex1: set1) {
					if (vertex.equals(vertex1)) {
						
						set1.remove(vertex1);
					//	System.out.println(vertex.hashCode()+"\t"+ vertex1.hashCode());
						flag =1;
						break;
					}
				}
				
				if (flag == 0) {
					first = false;
					break;

				}
			}
		}
		boolean second =true;
		
		for (EAGVertex vertex: set1) {
			int flag = 0;
			
			for (EAGVertex vertex1: set) {
				if (vertex.equals(vertex1)) {
					
					set.remove(vertex1);
				//	System.out.println(vertex.hashCode()+"\t"+ vertex1.hashCode());
					flag =1;
					break;
				}
			}
			
			if (flag == 0) {
				second = false;
				break;
			}
		}

		
		return first && second;
	}


	
	
	public void setProjection(
			HashMap<String, HashSet<String>> toProject) {
		
		projectingVariables.putAll(toProject);
	}
	
	public HashMap<String, HashSet<String>> getProjection() {
		return projectingVariables;
	}
	/*//@Override
	public int hashCode() {
		return this.extendedGraph.getTripleIdMap().values().hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		
		return (this.extendedGraph.getTripleIdMap().values().equals(((EquivalenceNode)obj).getEAGraph().getTripleIdMap().values()));

	}
*/
}
