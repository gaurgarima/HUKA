package preprocessing.queryRegistry.executionPlan.local;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map.Entry;

public class AndOrGraph implements java.io.Serializable{

	private HashMap<Integer, EquivalenceNode> equivNodeList;
	private HashMap<Integer, OperationNode> opNodeList;
	private ArrayList<Integer> equivNodeIdRange;
	private ArrayList<Integer> opNodeIdRange;
	private HashMap<Integer, HashSet<Integer>> levelENodeMap;
	private HashMap<Integer, HashSet<Integer>> levelOpNodeMap;
	private Integer size;
	private EquivalenceNode rootNode;

	
	public AndOrGraph() {
		this.equivNodeList =new HashMap();
		this.opNodeList = new HashMap();
		this.levelENodeMap = new HashMap();
		this.levelOpNodeMap = new HashMap();
		this.size = 0;
	}
	
	public AndOrGraph (AndOrGraph g) {
		
		this.equivNodeList =new HashMap();
		this.opNodeList = new HashMap();
		this.levelENodeMap = new HashMap();
		this.levelOpNodeMap = new HashMap();
		this.equivNodeIdRange = new ArrayList();
		this.opNodeIdRange = new ArrayList();


		for (Entry e: g.getENodeList().entrySet()) {
			
			EquivalenceNode valE = (EquivalenceNode) e.getValue();
			EquivalenceNode eNode = new EquivalenceNode(valE.getId(), valE.getMap(), valE.isRoot(),"0");
					 
			Iterator it =valE.getChildrenList().iterator();
			
			while(it.hasNext()) {
				eNode.addChild((Integer) it.next());
			}
			
			Iterator itr = valE.getParentList().iterator();
			
			while(itr.hasNext()) {
				eNode.addParent((Integer)itr.next());
			}
			
			equivNodeList.put((Integer)e.getKey(), eNode);
		}
		

		this.opNodeList.putAll(g.getOpNodeList());
		this.size =g.getSize();
		this.rootNode = g.getRoot();
		
		buildLevelMap();


	} 
	
	
	public AndOrGraph(HashMap<Integer, EquivalenceNode> eNodeMap, HashMap<Integer, OperationNode> opNodeMap, int startE, int endE, int startO, int endO, int size, EquivalenceNode root) {
		
		this.equivNodeList =new HashMap();
		this.opNodeList = new HashMap();
		this.levelENodeMap = new HashMap();
		this.levelOpNodeMap = new HashMap();
		this.equivNodeIdRange = new ArrayList();
		this.opNodeIdRange = new ArrayList();
		
		this.equivNodeList.putAll(eNodeMap);
		this.opNodeList.putAll(opNodeMap);
		this.equivNodeIdRange.add(startE);
		this.equivNodeIdRange.add(endE);
		this.opNodeIdRange.add(startO);
		this.opNodeIdRange.add(endO);
		this.size =size;
		this.rootNode = root;

		buildLevelMap();
	}
	
	public ArrayList<Integer> getENodeRange() {
		return this.equivNodeIdRange;
	}
	
	public ArrayList<Integer> getOpNodeRange() {
		return this.opNodeIdRange;
	}
	
	private void buildLevelMap() {
		//System.out.print("Szie: "+size);
		
		for (int i = 1; i <= size; i++) {
			HashSet<Integer> set = new HashSet();
			HashSet<Integer> set1 = new HashSet();

			
			for(Entry<Integer, EquivalenceNode> entry: equivNodeList.entrySet()) {
				
				if (entry.getValue().getLevel() == i) {
					set.add(entry.getValue().getId());
				}
			}
			
			for(Entry<Integer, OperationNode> entry: opNodeList.entrySet()) {
				
				if (entry.getValue().getLevel() == i) {
					set1.add(entry.getValue().getId());
				}
			}
 
			levelENodeMap.put(i, set);
			levelOpNodeMap.put(i, set1);
		}
		
		HashSet<Integer> topLevelId = new HashSet();
		topLevelId.addAll(levelENodeMap.get(size));
		
		
		if (topLevelId.size()>1 || topLevelId.size() == 0) {
			System.out.println("Error: More than 2 roots found !!");
			System.out.println("for root "+rootNode.getQueryIdList().toString());
			System.exit(0);
		
		} else {
			Integer id = topLevelId.iterator().next();
			equivNodeList.get(id).setRoot();
			equivNodeList.get(id).addQueryIds(rootNode.getQueryIdList());
			equivNodeList.get(id).putQVarMap(rootNode.getQVarMap());	
			equivNodeList.get(id).setProjection(rootNode.getProjection());
		}

		
	}
	
	public EquivalenceNode getRoot() {
		return rootNode;
	}
	
	
	public void buildLevelMapBeforeShow() {
		
		for (int i = 1; i <= size; i++) {
			HashSet<Integer> set = new HashSet();
			HashSet<Integer> set1 = new HashSet();

			
			for(Entry<Integer, EquivalenceNode> entry: equivNodeList.entrySet()) {
				
				if (entry.getValue().getLevel() == i) {
					set.add(entry.getValue().getId());
				}
			}
			
			for(Entry<Integer, OperationNode> entry: opNodeList.entrySet()) {
				
				if (entry.getValue().getLevel() == i) {
					set1.add(entry.getValue().getId());
				}
			}
 
			levelENodeMap.put(i, set);
			levelOpNodeMap.put(i, set1);
		}
		
	}
	
	public void show() {
		
		buildLevelMapBeforeShow();
		
		
	/*
	 * Show all the operation nodes	
	 */
	
		System.out.println("\n ENOdes: \n");

		for (Integer i : equivNodeList.keySet()) {
			System.out.println("E"+i+"\n C:"+equivNodeList.get(i).getChildrenList().toString()+"\nP:"+equivNodeList.get(i).getParentList().toString()+"\n");
		}		
	
		for (Integer i: opNodeList.keySet()) {
			System.out.println("Op: "+i+"\nC:"+opNodeList.get(i)+"\nP:"+opNodeList.get(i)+"\n");
		}


		for (Integer i : opNodeList.keySet()) {
			System.out.println(i+"\t"+opNodeList.get(i).getChildrenList().toString()+
				"\t - ("+opNodeList.get(i).getOperator()+") -> \t"+opNodeList.get(i).getParentList().toString()+
				"\t"+opNodeList.get(i).getLeftOperand().get(0).getId()+", "+opNodeList.get(i).getRightOperand().get(0).getId());
		}
	
		System.out.println("\n");
	}
	
	
	public int getEnodeCount() {
		return equivNodeList.size();
	}

	public int getOpNodeCount() {
		return opNodeList.size();
	}

	public HashMap<Integer, EquivalenceNode>  getENodeList() {
		return equivNodeList;
	}
	
	public HashMap<Integer, OperationNode>  getOpNodeList() {
		return opNodeList;
	}

	public void addENode(EquivalenceNode eNode, Integer eNodeId) {

		equivNodeList.put(eNodeId, eNode);
	}
	
	public void addOpNode(OperationNode eNode, Integer eNodeId) {

		opNodeList.put(eNodeId, eNode);
	}
	
	
	public Integer getSize(){
		return size;
	}
	
	public void setSize(Integer size){
		this.size = size;
	}

	public HashMap<Integer, HashSet<Integer>> getLevelENodeMap(){
		return levelENodeMap;
	}
	
	public HashMap<Integer, HashSet<Integer>> getLevelOpNodeMap(){
		return levelOpNodeMap;
	}

	public Object getQueryList() {

		return rootNode.getQueryIdList();
	}
}
