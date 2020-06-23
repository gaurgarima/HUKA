package preprocessing.queryRegistry.executionPlan.local;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Set;

/**
 * This is an abstract class which serves as the base class for the equivalence and operation node of 
 * AND-OR Graph.
 * @author garima
 *
 */

public abstract class AndOrGraphNode implements java.io.Serializable {
	Integer id;

	//LinkedList<Integer> parentList;
	//LinkedList<Integer> childrenList;

	LinkedHashSet<Integer> parentList;
	LinkedHashSet<Integer> childrenList;
	String nodeType;
	Integer level;

	
	public AndOrGraphNode() {
//		parentList = new LinkedHashSet<Integer>();
//		childrenList = new LinkedHashSet<Integer>();
		parentList = new LinkedHashSet<Integer>();
		childrenList = new LinkedHashSet<Integer>();
	
		
		nodeType = new String();
	}
	
	public void setLevel(Integer level) {
		this.level =level;
	}
	
	public void addParent(Integer parentId) {
		parentList.add(parentId);
	}
	
	public void addChild(Integer childId) {
		childrenList.add(childId);
	}
	
	public void setNodeType( String type) {
		nodeType = type;
	}
	
	public void setId(Integer id){
		this.id = id;
	}
	public Integer getLevel() {
		return this.level;
	}
	
	public String getNodeType(){
		return this.nodeType;
	}
	
	public LinkedHashSet<Integer> getParentList() {
		return this.parentList;
	}
	
	public LinkedHashSet<Integer> getChildrenList() {
		return this.childrenList;
	}
	
	public Integer getParentCount() {
		return this.parentList.size();
	}
	
	public Integer getChildrenCount() {
		return this.childrenList.size();
	}
	
	public Integer getId() {
		return this.id;
	}
	
	public void clearChildrenList() {
		this.childrenList.clear();
	}
	
	public void clearParentList() {
		this.parentList.clear();
	}
	
}
