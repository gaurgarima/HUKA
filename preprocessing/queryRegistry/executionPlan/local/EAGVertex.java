package preprocessing.queryRegistry.executionPlan.local;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * This class object represents node in an extended abstract graph of each abstract graph
 * @author garima
 *
 */
public class EAGVertex implements java.io.Serializable {
	private Integer id;
	private String subject;
	private String object;
	private String relation;
	private String subjectIdentifier;
	private String objectIdentifier;
	private String relationIdentifier;
	private String abstractForm;
	//private HashSet<String> neighborsList; //why is the neighbor list hashset, it should be arraylist since two neighbors can have similar abstract forms
	private HashSet neighborsList;
	
	public EAGVertex(Integer id, String form) {
		String[] formComp = form.split(", ");
		subject = formComp[0];
		relation = formComp[1];
		object = formComp[2];		
		this.id = id;
		this.abstractForm = form;
		this.neighborsList = new HashSet();	//TO-DO: this has not been used anywhere, the neighbors of each EAGVertex is maintained in the ExtendedAbstractGraph object
		// this 
	}
	
	//@Override
	public int hashCode() {
		return this.abstractForm.hashCode() + this.neighborsList.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
			if (this.neighborsList.isEmpty() && ((EAGVertex)obj).neighborsList.isEmpty()) {
				return this.abstractForm.equals(((EAGVertex)obj).abstractForm);	
			}
			Set<EAGVertex> set = new HashSet(neighborsList);
			Set<EAGVertex> set1 = new HashSet(((EAGVertex)obj).neighborsList);

			
			//return (this.abstractForm.equals(((EAGVertex)obj).abstractForm) && this.neighborsList.equals(((EAGVertex)obj).neighborsList));
			return (this.abstractForm.equals(((EAGVertex)obj).abstractForm) && this.neighborsList.size() == ((EAGVertex)obj).neighborsList.size() && set.equals(set1));		

	}
		

	public Integer getId() {
		return this.id;
	}
	
	public String getForm() {
		return abstractForm;
	}
	
	public String getSubject() {
		  return subject;
	}
	  
	public String getObject() {
		return object;
	}
	  
	 public String getRelation() {
		  return relation;
	 }
	 
	 public HashSet getNeighborList() {
		 return this.neighborsList;
	 }
	 
	 public void setSubjectIdentifier(String id) {
		  this.subjectIdentifier = id;
	 }
	  
	 public void setObjectIdentifier(String id) {
		  this.objectIdentifier = id;
	 }

	 public void setRelationIdentifier(String id) {
		  this.relationIdentifier = id;
	 }
	  
	 public String getSubjectIdentifier() {
		  return this.subjectIdentifier;
	 }
	  
	 public String getObjectIdentifier() {
		  return this.objectIdentifier;
	 }
	 
	 public void setNeighbors(ArrayList<String> neighbors) {
		 this.neighborsList.addAll(neighbors);
	 }
	 
}
