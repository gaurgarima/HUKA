package preprocessing.estimator;

import java.util.LinkedHashSet;
import java.util.Set;

public class ModifiedPredicate implements java.io.Serializable{

		String id;
		Set<String> objSet;
		int count = 0;
		
		public ModifiedPredicate(String name){
			
			id = name;
			objSet = new LinkedHashSet();
		}
		
		public ModifiedPredicate(String name, Set<String> set, int c){
			
			id = name;
			objSet = set;
			count = c;
		}
		
		public ModifiedPredicate(Predicate p) {

			this.id = p.id;
			this.count = p.count;
			this.objSet = new LinkedHashSet();
			this.objSet.addAll(p.get_ObjSet());
		}

		Set<String> get_ObjSet(){
			return objSet;
		}
		
		int get_count(){
			return count;
		}
		
		void incr_count(){
			count++;
		}
		
		void merge_objSet(Set set){
			objSet.addAll(set);
		}
		
		void update_counter(int i){
			count = count + i;
		}
		
		void add_obj(String obj){
			objSet.add(obj);
		}
	}
