package preprocessing.queryRegistry.executionPlan.global;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

public class InternalRelation implements java.io.Serializable {
	
	String id;
	//ArrayList<String> operation;	// op1:op2 join on these attributes
	ArrayList<String> leftAttributeList;
	ArrayList<String> rightAttributeList;
	String leftChildId;
	String rightChildId;
	LinkedList<String> leftOperandAttribute;
	LinkedList<String> rightOperandAttribute;
	ArrayList<String> parentIdList;
	String operation;
	boolean isRoot;
	HashSet<String> queryList;
	HashMap<String, HashMap<String, String>> QVarToAttributeMap;
	HashMap<String, HashMap<String, String>> tempAttributeToQVarMap;
	HashMap<String, HashSet<String>> toProject;
	
	public InternalRelation (String id, boolean rootFlag) {
		
		this.id = id;
		isRoot = rootFlag;
		//operation = new ArrayList();
		operation = new String();
		leftAttributeList = new ArrayList();
		rightAttributeList = new ArrayList();
		leftOperandAttribute = new LinkedList();
		rightOperandAttribute = new LinkedList();
		parentIdList = new ArrayList();
		QVarToAttributeMap = new HashMap();	// Query variable to the internal node attribute name (after name correction)
		tempAttributeToQVarMap = new HashMap(); // Internal Node attribute (old names) to qVariable name (this is used for transition from AndOrGraph to Internal node)
		toProject = new HashMap<String, HashSet<String>>();
		queryList = new HashSet();
	}
	
	/*public void addOperation (String str) {
		
		operation.add(str);
	}*/
	
	public void addQueryId (ArrayList<String> qId) {
		queryList.addAll(qId);
	}
	
	public HashSet<String> getQueryIdList() {
		return queryList;
	}
	
	public void setProjection(HashMap<String, HashSet<String>> list) {
		toProject.putAll(list);
	}
	
	public HashMap<String, HashSet<String>> getProjection() {
		return toProject;
	}
	
	
	public void addQVarMap(String qId,HashMap<String,String> map) {
		
		if (QVarToAttributeMap.containsKey(qId)) {
			QVarToAttributeMap.get(qId).putAll(map);
		
		} else {
			QVarToAttributeMap.put(qId, map);
		}
	}
	
	public void addQVarMapEntry(String qId,String key, String val) {
		
		if (QVarToAttributeMap.containsKey(qId)) {
			QVarToAttributeMap.get(qId).put(key, val);
		} else {
			
			HashMap<String, String> tempMap = new HashMap();
			tempMap.put(key, val);
			QVarToAttributeMap.put(qId, tempMap);
			
		}
	}
	
	
	public void addtempQVarMap(String qId, HashMap<String,String> map) {
		
		if (tempAttributeToQVarMap.containsKey(qId)) {
			tempAttributeToQVarMap.get(qId).putAll(map);
		
		} else {
			tempAttributeToQVarMap.put(qId, map);

		}
	}

	public String getAttribtueName (String qId, String qVar) {
		return QVarToAttributeMap.get(qId).get(qVar);
	}
	
	public HashMap<String,HashMap<String,String>> getQVarMap() {
		return QVarToAttributeMap;
	}
	
	public String gettempQVarEntry (String qId, String attr) {
		return tempAttributeToQVarMap.get(qId).get(attr);
	}
	
	
	public HashMap<String, HashMap<String, String>> gettempQVar () {
		return tempAttributeToQVarMap;
	}

	
	public boolean isRoot() {
		return isRoot;
	}
	
	public String getId() {
		return id;
	}
	
	public void setOperation(String op) {
		this.operation = op;
	}
	
	
	public void addLeftAttributeList (ArrayList<String> list) {
		leftAttributeList.addAll(list);
	}
	
	public void addRightAttributeList (ArrayList<String> list) {
		rightAttributeList.addAll(list);
	}

	public void setLeftChild (String id) {
		leftChildId = id;
	}
	
	public void setRightChild (String id) {
		rightChildId = id;
	}
	
	public void setLeftOperandAttribute (LinkedList<String> s) {
		this.leftOperandAttribute.addAll(s);
	}
	
	public void setRightOperandAttribute (LinkedList<String> s) {
		this.rightOperandAttribute.addAll(s);
	}
	
	public void addParentId (String id) {
		this.parentIdList.add(id);
	}
	
	public ArrayList<String> getParentIdList() {
		return this.parentIdList;
	}
	
	public String getLeftChildId() {
		return this.leftChildId;
	}
	
	public String getRightChildId() {
		return this.rightChildId;
	}
	
	public String getOperation()  {
		return this.operation;
	}
	
	public ArrayList<String> getLeftAttribute() {
		return this.leftAttributeList;
	}
	
	public ArrayList<String> getRightAttribute() {
		return this.rightAttributeList;
	}
	
	public LinkedList<String> getLeftOpAttribute() {
		return this.leftOperandAttribute;
	}
	
	public LinkedList<String> getRightOpAttribute() {
		return this.rightOperandAttribute;
	}
	
	
	

	
	
}
