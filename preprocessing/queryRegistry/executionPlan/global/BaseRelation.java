package preprocessing.queryRegistry.executionPlan.global;

import java.util.ArrayList;
import java.util.HashMap;

public class BaseRelation implements java.io.Serializable{

	String id;
	String relationId;
	ArrayList<String> paths;
	HashMap<String, String> pathToConditionMap;
	String parentId;
	String constraint;
	
	
	public BaseRelation() {
	

		this.id = new String();
		this.relationId = new String();
		this.paths = new ArrayList();
		this.pathToConditionMap = new HashMap();
		this.parentId = new String();
		this.constraint = new String();
	}
	
	public BaseRelation (String id, String rel) {
		
		this.id = new String();
		this.relationId = new String();
		this.id = id;
		this.relationId = rel;
		this.paths = new ArrayList();
		this.pathToConditionMap = new HashMap();
		this.parentId = new String();
		this.constraint = new String();
	}
	
	public void setConstraint (String str) {
		constraint = str;
	}

	public String getConstraint () {
		return constraint;
	}
	
	public boolean hasConstraint() {
		
		if (constraint.length() > 0)
			return true;
		else
			return false;
	}
	
	
	public void addPath (String path) {
		paths.add(path);
	}
	
	public ArrayList<String> getPathList() {
		return paths;
	}
	
	public void addPathCondition (String path, String cond) {
		
		pathToConditionMap.put(path, cond);
	}
	
	public String getCondition(String path) {
		
		return pathToConditionMap.get(path);
	}
	
	public void setParent(String id) {
		this.parentId = id;
	}
	
	public String getParentId() {
		return this.parentId;
	}
	
	public String getRelation() {
		return this.relationId;
	}
	

}
