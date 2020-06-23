package preprocessing.estimator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;

public class Star {

	String subject;
	
	ArrayList<String> outgoingEdges;
	HashMap<String, ArrayList<String>> objectToPred;
	
	//HashMap<String, String> outgoingEdges;
	
	public Star(String s){
		
		subject=s;
		outgoingEdges=new ArrayList();
		objectToPred = new HashMap();
	}
	
	void add_Edges(String pred, String obj){
		
		outgoingEdges.add(pred.concat(" : ").concat(obj));
		
		if(objectToPred.containsKey(obj))
			objectToPred.get(obj).add(pred);
		else
			objectToPred.put(obj, new ArrayList(Arrays.asList(pred)));
	}
	
	HashMap<String, ArrayList<String>> get_ObjectToPredicate(){
		return objectToPred;
	}
	
	String get_CS(){
		
	LinkedHashSet<String> predList=new LinkedHashSet();
	
	for(String s: outgoingEdges){
		
		String[] scomp=s.split(" : ");
		
		predList.add(scomp[0]);
	}
	return predList.toString();
		
	}
	
	LinkedHashSet<String> getNeighbors(){
		
		LinkedHashSet<String> neighborList=new LinkedHashSet();
		
		for(String s: outgoingEdges){
			
			String[] scomp=s.split(" : ");
			
			neighborList.add(scomp[1]);
		}
		return neighborList;
	}

	ArrayList<String> getOutgoingEdges(){
		
		return outgoingEdges;
	}
	
	LinkedHashSet<String> outgoingEdgeLabels(){
		
		LinkedHashSet<String> edgeList=new LinkedHashSet();
		
		for(String s: outgoingEdges){
			
			String[] scomp=s.split(" : ");
			
			edgeList.add(scomp[0]);
		}
		return edgeList;
	}
	
	String getCenter() {
		return subject;
	}

}

