package preprocessing.queryRegistry.queryExecutor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

public class PolynomialBuilder {

	
	String id=new String();
	HashMap<String,String> result_set=new HashMap();	//RESULT,POLYNOMIAL
	HashMap<String,String> Id_to_def_ID=new HashMap();
	Multimap<String,List<String>> result=ArrayListMultimap.create();
	int value=0;
	
	
	Boolean has_build=false;
	
	public PolynomialBuilder( HashMap<String,String> ids, Multimap<String,List<String>> res)
	{
		Id_to_def_ID.putAll(ids);
		result.putAll(res);
		
	}
	
	
	public HashMap<String,String> constructProvPoly(){
		
		Set<String> result_item=result.keySet();
		String poly=new String();

		for(String str: result_item){

			poly = build((List<List<String>>) result.get(str));
			result_set.put(str,poly);
		}
		
		has_build=true;
		
		return result_set;
		
	}
	
	
	private String build(List<List<String>> paths ){
		
		String edge_id=new String();
		String term=new String();		
		String polynomial=new String();

		HashMap<String,Integer> addends=new HashMap(); // Maintains pair of addedn/term and its coefficient

		for(List<String>path: paths){ 	 	//Multiple paths may exist corresponding to one result item
		
			term="";
			for(String edge: path){
				
				edge_id=Id_to_def_ID.get(edge);
				term=term.concat("e"+edge_id); //appending 'e' with id and constructing an addend/term
					
			}	//end of for-edge
			
			term=sort(term); //reconstruct the term by arranging edge ids in increasing order
			
			//Update coefficient if same addend is already present in polynomial
			if(addends.containsKey(term))
			{
				int coefficient=addends.get(term);
				coefficient++;
				addends.put(term, new Integer(coefficient));
			}
			else
				addends.put(term, 1);
		}	//end of for-path
		
		Set<String> addend_set=addends.keySet();
		//Constructing final polynomial string
		for(String str: addend_set ){
			
			if(addends.get(str) != 1)
				polynomial=polynomial.concat(addends.get(str).toString()+str+"+");
			else
				polynomial=polynomial.concat(str+"+");
			
		} 
		
		polynomial=polynomial.substring(0,polynomial.length()-1); //to remove extra "+"
		
		//System.out.println("************* Poly construction done!!");
		return polynomial;
			
	}	//end of build()
	
	
	private String sort(String path){		//Change integers to long; ids can be > int range

		String edges[]=path.split("e");
		List<Long> edge_ids=new ArrayList();
	//	System.out.print("\nP: "+path);
		for(String e: edges){
	
			if(!e.equals(""))
			edge_ids.add(Long.parseLong(e));
		}
		
		Collections.sort(edge_ids);
		path="";
		
		for(Long i: edge_ids)
			path=path.concat("e"+i.toString());
			
		return path;	
	}
	
}
