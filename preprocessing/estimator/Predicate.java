package preprocessing.estimator;

import java.util.LinkedHashSet;
import java.util.Set;

public class Predicate implements java.io.Serializable{
	
	String id;
	Set<String> objSet;
	int count = 0;
	
	public Predicate(String name){
		
		id = name;
		objSet = new LinkedHashSet();
	}
	
	public Predicate(String name, LinkedHashSet<String> set, int c){
		
		id = name;
		objSet = set;
		count = c;
	}
	
	public Predicate(Predicate p) {

		this.id = p.id;
		this.count = p.count;
		this.objSet = new LinkedHashSet();
		this.objSet.addAll(p.get_ObjSet());
	}

	public Set<String> get_ObjSet() {
		return objSet;
	}
	
	public int get_count() {
		return count;
	}
	
	void incr_count() {
		count++;
	}
	
	void add_obj(String obj){
		
		objSet.add(obj);
	}
}
