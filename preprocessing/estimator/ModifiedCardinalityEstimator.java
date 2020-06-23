package preprocessing.estimator;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.syntax.ElementPathBlock;
import org.apache.jena.sparql.syntax.ElementVisitorBase;
import org.apache.jena.sparql.syntax.ElementWalker;

import preprocessing.queryRegistry.executionPlan.local.TriplePattern;

public class ModifiedCardinalityEstimator {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	
	static FileWriter fw ;
	static BufferedWriter  bw;
	static String query_id;
	
	 /*
	static HashMap<String, HashMap<String,Integer>> csPair =new HashMap();
	static HashMap<String,Integer> csPairCount = new HashMap();
	*/
	
	//static HashMap<HashSet<String>, HashMap<String, Integer>> cs = new HashMap();

	static HashMap<HashSet<String>, HashMap<String, ModifiedPredicate>> cs = new HashMap();
	static HashMap<Set<String>, Integer> cs_distinct= new HashMap();
	static HashMap<ArrayList<HashSet<String>>, HashMap<String, Integer>> csPair =new HashMap();
	
	static HashMap<ArrayList<HashSet<String>>, Integer> csPair_distinct =new HashMap();
	
	static HashMap<String, Star> starList;


	
	public static void main(String[] args) throws ClassNotFoundException, IOException {
		// TODO Auto-generated method stub
		
		
	//	FileInputStream fi2 = new FileInputStream(new File("./test/dummy_CSPair.ser"));

		FileInputStream fi2 = new FileInputStream(new File("./final/CSPair.ser"));
		ObjectInputStream oi2 = new ObjectInputStream(fi2);
		csPair= (HashMap<ArrayList<HashSet<String>>, HashMap<String, Integer>>) oi2.readObject();
		oi2.close();
		fi2.close();
		
	//	FileInputStream fi3 = new FileInputStream(new File("./final/dummy_CSPairCount.ser"));

		FileInputStream fi3 = new FileInputStream(new File("./final/CSPairCount.ser"));
		ObjectInputStream oi3 = new ObjectInputStream(fi3);
		csPair_distinct= (HashMap<ArrayList<HashSet<String>>, Integer>) oi3.readObject();
		oi3.close();
		fi3.close();
		
		int count=0;
		
		for(ArrayList<HashSet<String>> s: csPair_distinct.keySet()){
			
			if(csPair_distinct.get(s) == 1){
				
				count++;
		//		csPair.remove(s);
			//	csPairCount.remove(s);
			}
		}
		
		System.out.println("\n %%%% "+count+"\t"+ csPair_distinct.size()); //  %%%% 185127	289454


		String  queryEstFile="queryEstimateRecord_avg.txt";
		
//		queryEstFile ="./final/queryEstimateRecord.txt";
		
	//	String  queryEstFile="yago_query_estimate_trial.txt";

		File file =new File(queryEstFile);

    	if(!file.exists())
    	   file.createNewFile();
    		
    	 fw = new FileWriter(file,true);
    	  bw = new BufferedWriter(fw);
    	  
    	//  BufferedReader br = new BufferedReader(new FileReader("query_collection_yago_old.txt"));
    	 
    	  BufferedReader br = new BufferedReader(new FileReader("./experiments/yago_query_collection.txt"));

    	  
    	  String query_stmt=new String();

    	  while((query_stmt=br.readLine())!= null){
    		  
    	//	  System.out.println(query_stmt);
    		  
    		 if(!query_stmt.startsWith("*")){
    			 
    			  query_id= query_stmt.split("\t")[0];
        		  
        		  buildStar(query_stmt.split("\t")[1]);
    		 }
    	
   		  System.out.println("@@@@@@");

    		 // bw.write(query_id+"\n"+query_stmt+"\n");
    	  
    		  
    	  }

    	  bw.close();
    	  br.close();
			System.out.println("Done!!");

/*		String query_stmt="BASE <http://yago-knowledge.org/resource/> "+
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
				"SELECT ?person " +
				"WHERE {" +
				"      ?person rdf:type <wordnet_scientist_110560637>. " +
				"      ?person <hasAcademicAdvisor> ?advisor. "+
				"	   ?person <wasBornIn> ?pcity."+
				" 	   ?advisor <wasBornIn> ?acity. "+
				" 	   ?pcity <isLocatedIn> <Switzerland>. "+
				"  	   ?acity <isLocatedIn> <Germany>."+
				"		}";
		
	/*	query_stmt="BASE <http://yago-knowledge.org/resource/> "+
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
				"SELECT ?person " +
				"WHERE {" +
				"      ?person rdf:type <wordnet_scientist_110560637>. " +
				"      ?person <hasAcademicAdvisor> ?advisor. "+
				"	   ?person <wasBornIn> ?pcity."+
				" 	   ?advisor <wasBornIn> ?acity. "+
							"		}";
		*/
	/*	query_stmt="BASE <http://yago-knowledge.org/resource/> "+
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
				"SELECT ?person " +
				"WHERE {" +
				"      ?person rdf:type ?type. " +
				"      ?person <hasAcademicAdvisor> ?advisor. "+
				" 	   ?advisor <isMarriedTo> ?acity. "+
				"	   ?person <wasBornIn> ?pcity."+
				"		}";
		
		//******* dummy
		
		 query_stmt="BASE <http://yago-knowledge.org/resource/> "+
					"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
					"SELECT ?person " +
					"WHERE {" +
					"      ?person <type> ?type. " +
					"      ?person <friendOf> ?friend. "+
					"	   ?person  <memberOf>?group."+
					"		}";
	
		
		query_stmt="BASE <http://yago-knowledge.org/resource/> "+
					"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
					"SELECT ?person " +
					"WHERE {" +
					"      ?person <type> ?type. " +
					"      ?person <friendOf> ?friend. "+
					"	   ?friend  <memberOf>?group."+
					" 	   ?group <follows> ?pg. "+
					" 	   ?friend <likes> ?pg1. "+
					"		}";
		 
	*/
		 
	//	 query_stmt="BASE <http://yago-knowledge.org/resource/> PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> SELECT ?student ?adv WHERE { ?student <hasAcademicAdvisor> ?adv. ?student <hasWonPrize> ?prize.  ?adv <hasWonPrize> ?prize1.}";
		 //Estimated Size: 89638.586

		 
		 
		 //***********
		
		
	//	query_stmt="BASE <http://yago-knowledge.org/resource/> PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> select ?s ?l ?u ?c ?m where { ?s <hasOfficialLanguage> ?l.  ?s <hasCapital> ?c. ?s <hasCurrency> ?m }";
		
	/*	
		query_stmt="BASE <http://yago-knowledge.org/resource/> PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> select ?s ?l ?t where { ?s <wasBornIn> ?l.?s rdf:type ?t. }";
//	281067	2546449
		
		//query_stmt="BASE <http://yago-knowledge.org/resource/> PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> select ?s ?c ?m where { ?s <hasOfficialLanguage> <French_language>. ?s <hasCapital> ?c. ?s <hasCurrency> ?m }";
//	query_stmt="BASE <http://yago-knowledge.org/resource/> PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> select ?s ?d where { ?s <diedIn> ?city. ?s <diedOnDate> ?d. ?s rdf:type ?t }";
	// 777446(true)	1554892	
	
/*	query_stmt = "BASE <http://yago-knowledge.org/resource/> "+
			"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
			"SELECT ?person1 ?person2 " +
			"WHERE {" +
			"      ?person1 <wasBornIn> ?city. " +
			"      ?person2 <wasBornIn> ?city. "+
			"	   ?person1 <isMarriedTo> ?person2."+
			"		}";
	//		9057.835	856
*/
//	buildStar(query_stmt);
	}

	static void buildStar(String query_stmt) throws ClassNotFoundException, IOException{
		
		/*
		 * construct the query, save its all triple in a list and one by one fetch them
		 */
		
		//String query_stmt="";
		Query query= QueryFactory.create(query_stmt);
		
	//	HashMap<String, Star> starList= new HashMap();
		
		final ArrayList<TriplePath> tripleList=new ArrayList();
		
		ElementWalker.walk(query.getQueryPattern(),
			    // For each element...
			    new ElementVisitorBase() {
			        // ...when it's a block of triples...
			        public void visit(ElementPathBlock el) {
			            // ...go through all the triples...
			            Iterator<TriplePath> triples = el.patternElts();
			            while (triples.hasNext()) {
			                // ...and grab the subject
	           	
			            	TriplePath triple= new TriplePath(triples.next().asTriple());
			            	
			            	tripleList.add(triple);
			            }
			        }
			    }
			);
		 starList= new HashMap();

		 
		 
		
		 
		for(TriplePath triple: tripleList){
			
			String sub= triple.getSubject().toString();
			String pred = triple.getPredicate().toString();
			String obj= triple.getObject().toString();
			
		//	pred= pred.substring(pred.lastIndexOf("/")+1);
			
		if(pred.contains("#"))
			pred=pred.substring(pred.lastIndexOf("#")+1);
			
		if(pred.equals("type"))
			pred="rdf:type";
		else
			if(pred.equals("subClassOf"))
				pred="rdfs:subClassOf";
			else
				pred= "<".concat(pred).concat(">");
				
		
		//	System.out.println(pred);
			
			if(starList.keySet().contains(sub)){
				
				starList.get(sub).add_Edges(pred, obj);
				
			}
			else{
				Star s= new Star(sub);
				s.add_Edges(pred, obj);
				
				starList.put(sub, s);
			}
			
		}
		
		//System.out.println("Number of stars in query: "+starList.size());
		
	/*	for(String s: starList.keySet()){
			
			
			System.out.println(s);
			System.out.println(starList.get(s).outgoingEdges.toString()+"\n");

			System.out.println(starList.get(s).get_CS()+"\n");

		}
		*/
		
		
		constructCSPair(starList);
	}

	
	public static void get_estimation(Collection<? extends TriplePattern> collection) throws ClassNotFoundException, IOException{
		
		starList= new HashMap();

		FileInputStream fi2 = new FileInputStream(new File("./final/CSPair.ser"));
		ObjectInputStream oi2 = new ObjectInputStream(fi2);
		csPair= (HashMap<ArrayList<HashSet<String>>, HashMap<String, Integer>>) oi2.readObject();
		oi2.close();
		fi2.close();
		
	//	FileInputStream fi3 = new FileInputStream(new File("./final/dummy_CSPairCount.ser"));

		FileInputStream fi3 = new FileInputStream(new File("./final/CSPairCount.ser"));
		ObjectInputStream oi3 = new ObjectInputStream(fi3);
		csPair_distinct= (HashMap<ArrayList<HashSet<String>>, Integer>) oi3.readObject();
		oi3.close();
		fi3.close();
		
		
			for(TriplePattern t: collection){
				
				String sub= t.getSubject(); 
				String pred = t.getRelation();
				String obj= t.getObject();
				//  (<http://yago-knowledge.org/resource/hasAcademicAdvisor>)
				
			//	pred= pred.substring(2, pred.length()-2 );
			//	pred= pred.substring(pred.lastIndexOf("/")+1);
				
			if(pred.contains("#"))
					pred=pred.substring(pred.lastIndexOf("#")+1);
				
				if(pred.equals("type"))
					pred="rdf:type";
				else
					if(pred.equals("subClassOf"))
						pred="rdfs:subClassOf";
					else
						pred= "<".concat(pred).concat(">");
					
			
			//	System.out.println(pred);
				
				if(starList.keySet().contains(sub)){
					
					starList.get(sub).add_Edges(pred, obj);
					
				}
				else{
					Star s= new Star(sub);
					s.add_Edges(pred, obj);
					
					starList.put(sub, s);
				}
				
			}
			
			System.out.println("Number of stars in query: "+starList.size());
			
			for(String s: starList.keySet()){
				
				
				System.out.println(s);
				System.out.println(starList.get(s).outgoingEdges.toString()+"\n");

				System.out.println(starList.get(s).get_CS()+"\n");

			}
			
			
			
			constructCSPair(starList);
		
		
	}
	
	
	static void constructCSPair(HashMap<String, Star> starList) throws ClassNotFoundException, IOException{
		
		HashSet<String> subjects= new HashSet(starList.keySet());
		
		HashMap<String, ArrayList<String>> queryCSPairs=new HashMap(); 
		/*
		 * queryCsPairs look like "subject : object" -> List of predicates which are combining the subject and the object 
		 */
		
		for(String sub: subjects){
			
			HashSet<String> neighbors= new HashSet(starList.get(sub).getNeighbors());
			neighbors.retainAll(subjects);
			
			if(neighbors.size()>0){
				
				HashMap<String, ArrayList<String>> objectToPred= new HashMap(starList.get(sub).get_ObjectToPredicate());
				
				for(String neighbor: neighbors){
					
				
					String csp_id=sub.concat(" : ").concat(neighbor);

					if(queryCSPairs.containsKey(csp_id)){
						
						
						for(String s: objectToPred.get(neighbor)){
							
							queryCSPairs.get(csp_id).add(s);

						}
					}
					else{
						
						ArrayList<String>list = new ArrayList();
						
						for(String s: objectToPred.get(neighbor)){
						
							list.add(s);
						}
						queryCSPairs.put(csp_id, list);
					}
				}
			}
		}
		
	//	System.out.println("CSPairs: "+queryCSPairs.size());
		estimate1(queryCSPairs, starList);
	}
	

	static void estimate1(HashMap<String,  ArrayList<String>> queryCSPairs, HashMap<String,Star> starList) throws IOException, ClassNotFoundException{
		
		
//		HashMap<String, HashMap<String, ModifiedPredicate>> CS= new HashMap();
		//HashMap<String, HashMap<String, Integer>> CS= new HashMap();

		HashMap<String, Integer> cs_distinct= new HashMap();
		
		FileInputStream fi = new FileInputStream(new File("./final/CS.ser"));
	//	FileInputStream fi = new FileInputStream(new File("./final/statistics/CS_mode.ser"));


//		FileInputStream fi = new FileInputStream(new File("./test/dummy_CS.ser"));
		ObjectInputStream oi = new ObjectInputStream(fi);
		cs= (HashMap<HashSet<String>, HashMap<String, ModifiedPredicate>>) oi.readObject();
		//	CS= (HashMap<String, HashMap<String, ModifiedPredicate>>) oi.readObject();
		oi.close();
		fi.close();
	

		FileInputStream fi1 = new FileInputStream(new File("./final/CScount.ser"));
		ObjectInputStream oi1 = new ObjectInputStream(fi1);
		cs_distinct= (HashMap<String, Integer>) oi1.readObject();
		oi1.close();
		fi1.close();
		
		
		HashMap<String, ArrayList<Set<String>>> starToCS =new HashMap();
		
		/*
		 * find the matching(superset of query CS) CS of dataset
		 */
		for(String starSub: starList.keySet()){
			
			String starCS= starList.get(starSub).get_CS();
			String[] cs_comp= starCS.substring(1, starCS.length()-1).split(", ");
			HashSet<String> cs_set=new HashSet(Arrays.asList(cs_comp));
			int distCount=0;

			
			ArrayList<Set<String>> csList= new ArrayList();
			
			for(Set<String> cs_id: cs.keySet()){
				
					if(cs_id.containsAll(cs_set)){
			
					csList.add(cs_id);
				}
				
			}
			starToCS.put(starSub, csList);

		}

		
		String center= new String();
		HashSet<String> centerSet= new HashSet();
		centerSet.addAll(starList.keySet());
		
		for(String str: starList.keySet()){
		
			int flag=0;
			for(String neighbor: starList.get(str).getNeighbors()){
				
				if(centerSet.contains(neighbor))
				{
					centerSet.remove(neighbor);
					
					if(centerSet.size()==1){
				
						flag=1;
						break;

					}
				}
			}
			
			if(flag==1)
				break;
			
		}
		
		if(centerSet.size()==1){
			
			center=centerSet.iterator().next();
		}
				
		ArrayList<HashMap<String, Set<String>>> partialResult = new ArrayList();

		for(Set<String> centerCS: starToCS.get(center)){
			
			HashMap<String, Set<String>> map = new HashMap();
			map.put(center, centerCS);
			partialResult.add(map);
		}
		
		ArrayList<HashMap<String, Set<String>>> result = cartesianProd(center, partialResult, starToCS, starList.get(center));
		
		System.out.println("Total valid pairs: "+result.size());
		
		int flag=0;
		
		/*
		for(HashMap<String, Set<String>> entry: result){
			
			flag++;
			
			if(flag<3){
				
				System.out.println(entry.keySet()+"\n"+entry.values());
				
			}
			else
				break;
		}
		*/
		//********88
		
		int c=0;
		/*System.out.println("\n\n*****Result is************\n");
		
		for(HashMap<String, String>entry: result){
	
			System.out.println("\n"+c+"th result");
			for(String key: entry.keySet()){
				System.out.println(key+"\t"+entry.get(key));
			}
		}
		
		for(HashMap<String, String> entry: result){
			
			if(entry.keySet().size() != starToCS.keySet().size())
				System.out.println("Size not equal!! \t"+ entry.size()+"\t"+starToCS.size());
		}
		
		System.out.println("\n\n*****Result is************\n");
*/
		/*
		 * HashMap<String, HashMap<String, ModifiedPredicate>> CS= new HashMap();
		HashMap<String, Integer> cs_distinct= new HashMap();
		static HashMap<String, HashMap<String,Integer>> csPair =new HashMap();
	static HashMap<String,Integer> csPairCount = new HashMap();
	
		
		 */
		Float estimatedSize= new Float(0.0);
		
		HashSet set = new HashSet();
		set.addAll(result);
		
		System.out.println("@@@ Result is: "+result.size()+"\t set size: "+set.size());
		
		for(HashMap<String, Set<String>> entry: result){
			
	//		for(String key: entry.keySet())
	//		System.out.println(starList.get(key).get_CS()+"\t"+entry.get(key));
			
			Float localEstimate = new Float(1.0);
			localEstimate= ((float) cs_distinct.get(entry.get(center)));
			HashMap<String,  HashSet<String>> redundantEdges= new HashMap();
			
			for(String cspair: queryCSPairs.keySet()){
				
				String comp[]= cspair.split(" : ");
				String sub= comp[0];
				String obj= comp[1];
				
				Set<String> subCS= entry.get(sub);
				Set<String> objCS=entry.get(obj);
				ArrayList<Set<String>> cs_pair_id =new ArrayList();
				
				cs_pair_id.add(subCS);
				cs_pair_id.add(objCS);
				
				//String cs_pair= subCS.concat(" : ").concat(objCS);
				
				
				//*****
				HashSet<String> redun_pred=new HashSet();
				
				redun_pred.addAll(queryCSPairs.get(cspair));
				
				if(redundantEdges.containsKey(sub)){
					
					redundantEdges.get(sub).addAll(redun_pred);
					
				}
				else
					redundantEdges.put(sub, redun_pred);
				
		//		System.out.println("*************\n");	

				for(String pred: queryCSPairs.get(cspair)){
				
			//	System.out.println(cspair+"\t"+pred+"\t"+(((float)csPair.get(cs_pair_id).get(pred)) / ((float) csPair_distinct.get(cs_pair_id))));
				
						
					localEstimate= localEstimate* (((float)csPair.get(cs_pair_id).get(pred)) / ((float) csPair_distinct.get(cs_pair_id)));
		
			}
				if(localEstimate==0)
					break;
			}
			
			for(String starid: starList.keySet()){
				
				Set<String> starcs= entry.get(starid);
				Star star= starList.get(starid);
		
				HashSet<String> outgoingEdges = new HashSet(star.outgoingEdgeLabels());
				
				if(redundantEdges.containsKey(starid))
					outgoingEdges.removeAll(redundantEdges.get(starid));
			//	System.out.println("*************\n");	

				for(String pred: outgoingEdges){

			//		System.out.println("Factor: "+pred+"\t"+( (float)cs.get(starcs).get(pred).get_count()) / ((float)cs_distinct.get(starcs)));
				
			//		System.out.println("Factor: "+pred+"\t"+( (float)cs.get(starcs).get(pred)));

		//			localEstimate= localEstimate * (( (float)cs.get(starcs).get(pred)));

					
				//	localEstimate= localEstimate * (( (float)CS.get(starcs).get(pred)) / ((float)cs_distinct.get(starcs)));

								localEstimate= localEstimate * (( (float)cs.get(starcs).get(pred).get_count()) / ((float)cs_distinct.get(starcs)));
				}
				
			}
			
			estimatedSize= estimatedSize + localEstimate;
		
		}

		System.out.println("Estimated Size: "+estimatedSize);
		
	//	bw.write(query_id+"\t"+estimatedSize+"\n");
	//	bw.flush();
		
		//********************************************************************************************************
		//********************************************************************************************************
		
	}

	private static ArrayList<HashMap<String, Set<String>>> cartesianProd(String center,
			ArrayList<HashMap<String, Set<String>>> partialResult, HashMap<String, ArrayList<Set<String>>> starToCS, Star  star) {
		
		ArrayList<HashMap<String, Set<String>>> forwardResult = null;
		HashSet<String> neighbors= new HashSet(star.getNeighbors());
		
		neighbors.retainAll(starToCS.keySet());
		
		
		if(neighbors.size()>0){
			
			
			for(String neighbor: neighbors){
				
				forwardResult= new ArrayList();
				System.out.println("center: "+center+"\t neighbor: "+ neighbor);

				for(HashMap<String, Set<String>> entry : partialResult){
					
					Set<String> centerCS= entry.get(center);
					
					for(Set<String> objCS: starToCS.get(neighbor)){
						
						ArrayList<Set<String>> csPair_id =new ArrayList<Set<String>>();
						csPair_id.add(centerCS);
						csPair_id.add(objCS);
						
						if(csPair.containsKey(csPair_id)){
							
							int flag=0;
							
							for(String s: star.get_ObjectToPredicate().get(neighbor)){
								
								if(!csPair.get(csPair_id).containsKey(s))
									flag=1;
							}
							
							if(flag==0){
								
							HashMap<String, Set<String>> map =new HashMap();
							map.putAll(entry);
						//	map.put(center, centerCS);
							map.put(neighbor, objCS);
							
							forwardResult.add(map);
							}
						}
						
					}
					
				}
				
			//	System.out.println("Before:  "+partialResult.size());

				partialResult.removeAll(partialResult);
				
			//	System.out.println(partialResult.size());
				
				if(forwardResult.size()>0)
				partialResult.addAll(cartesianProd(neighbor, forwardResult, starToCS, starList.get(neighbor)));
				else
					break;
				
			//	System.out.println("After: "+partialResult.size());

			}
			
			
			
			
		}
		else{
			
		//	System.out.println(center+"\t"+partialResult.get(0).keySet().toString());

			return partialResult;
		}
		
		//2079946	2079946	233520	282364	6544123
			
			//for(HashMap<String, String> map: forwardResult){
			
		//		System.out.println(center+"\t"+forwardResult.get(0).keySet().toString());
							
			//}
			
		
		return forwardResult;
	}


}