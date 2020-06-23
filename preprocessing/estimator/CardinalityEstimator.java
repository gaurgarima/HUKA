package preprocessing.estimator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import main.ConfigurationReader;
import preprocessing.queryRegistry.executionPlan.local.AndOrGraph;
import preprocessing.queryRegistry.executionPlan.local.EquivalenceNode;
import  preprocessing.queryRegistry.executionPlan.local.TriplePattern;



public class CardinalityEstimator {

	static HashMap<String, Star> starList;
	static HashMap<HashSet<String>, HashMap<String, ModifiedPredicate>> cs = new HashMap();
	static HashMap<String, Integer> CSDistinct= new HashMap(); // for yago
	//	HashMap<HashSet<String>, Integer> CSDistinct = new HashMap(); // for lubm

	static HashMap<ArrayList<HashSet<String>>, HashMap<String, Integer>> csPair =new HashMap();
	
	static HashMap<ArrayList<HashSet<String>>, Integer> csPair_distinct =new HashMap();
	
	public static void main (String[] args) throws IOException, ClassNotFoundException {
		
		  String globalGraphFileName = "./final/subquery-global-execution.ser";
		  AndOrGraph globalGraph = null;

		  FileInputStream file2 = new FileInputStream(globalGraphFileName);
		  ObjectInputStream in2 = new ObjectInputStream(file2);
		  
		  globalGraph = (AndOrGraph)in2.readObject();
		  
		  in2.close();
		  file2.close();
	//	  System.out.println("Glboal: "+globalGraph.getSize());
		  EquivalenceNode e = globalGraph.getENodeList().get(globalGraph.getLevelENodeMap().get(2).iterator().next());
	//	  System.out.println("Enode: "+e.getId());
	//	  System.out.println(get_estimation(e.getAbstractGraph().getTripleList()));
	
		
	}

	public CardinalityEstimator() throws IOException, ClassNotFoundException {
		
		if (cs.isEmpty()) {
			
			String csPairPath = ConfigurationReader.get("CS_PAIR");
			String csPairCountPath = ConfigurationReader.get("CS_PAIR_COUNT");

			FileInputStream fi2 = new FileInputStream(new File(csPairPath));
			ObjectInputStream oi2 = new ObjectInputStream(fi2);
			csPair= (HashMap<ArrayList<HashSet<String>>, HashMap<String, Integer>>) oi2.readObject();
			oi2.close();
			fi2.close();
			

			FileInputStream fi3 = new FileInputStream(new File(csPairCountPath));
			ObjectInputStream oi3 = new ObjectInputStream(fi3);
			csPair_distinct= (HashMap<ArrayList<HashSet<String>>, Integer>) oi3.readObject();
			oi3.close();
			fi3.close();
			
			String csPath = ConfigurationReader.get("CS");
			String csCountPath = ConfigurationReader.get("CS_COUNT");

			//HashMap<String, Integer> cs_distinct= new HashMap(); // for yago
		//	HashMap<HashSet<String>, Integer> CSDistinct = new HashMap(); // for lubm
			//cs = new HashMap();
			FileInputStream fi = new FileInputStream(new File(csPath));
		//	FileInputStream fi = new FileInputStream(new File("./final/statistics/CS_mode.ser"));


//			FileInputStream fi = new FileInputStream(new File("./test/dummy_CS.ser"));
			ObjectInputStream oi = new ObjectInputStream(fi);
			cs = (HashMap<HashSet<String>, HashMap<String, ModifiedPredicate>>) oi.readObject(); // for yago
			//HashMap<String, HashMap<String, ModifiedPredicate>> CS= (HashMap<String, HashMap<String, ModifiedPredicate>>) oi.readObject(); //for lubm
			oi.close();
			fi.close();
		
			FileInputStream fi1 = new FileInputStream(new File(csCountPath));

//			FileInputStream fi1 = new FileInputStream(new File("./final/lubm/CScount.ser"));
			ObjectInputStream oi1 = new ObjectInputStream(fi1);
		//	cs_distinct= (HashMap<String, Integer>) oi1.readObject(); //for yago
			CSDistinct= (HashMap<String, Integer>) oi1.readObject(); //for yago

		//	CSDistinct = (HashMap<HashSet<String>, Integer>) oi1.readObject();  // for lubm
			oi1.close();
			fi1.close();
			


			
		}
		
	/*	System.out.println("\nCS: "+cs.size());
		System.out.println("\nCS count: "+CSDistinct.size());
		*/
	}
	
	public static Float get_estimation(Collection<? extends TriplePattern> collection) throws ClassNotFoundException, IOException{
		
		
		// Testing
		
		starList= new HashMap();

		
		
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
					/*else
						pred= "<".concat(pred).concat(">");
					*/
							
				if(starList.keySet().contains(sub)){
					
					starList.get(sub).add_Edges(pred, obj);
					
				}
				else{
					Star s= new Star(sub);
					s.add_Edges(pred, obj);
					
					starList.put(sub, s);
				}
				
			}
			
		//	System.out.println("Number of stars in query: "+starList.size());
			
		/*	for(String s: starList.keySet()){
				
				
		//		System.out.println(s);
		//		System.out.println(starList.get(s).outgoingEdges.toString()+"\n");

		//		System.out.println(starList.get(s).get_CS()+"\n");

			}
			*/
			
			
			return constructCSPair(starList);
		
		
	}
	
	static Float constructCSPair(HashMap<String, Star> starList) throws ClassNotFoundException, IOException{
		
		for (String s: starList.keySet()) {
		//	System.out.println(s+"\t"+starList.get(s).getCenter());
		}
		
		//System.out.println("\n");
				
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
		
		//return estimate1(queryCSPairs, starList);
		return estimate2(queryCSPairs, starList); // After E19

	}
	
	static Float estimate1(HashMap<String,  ArrayList<String>> queryCSPairs, HashMap<String,Star> starList) throws IOException, ClassNotFoundException{
		
		

		HashMap<String, Integer> cs_distinct= new HashMap();
		cs = new HashMap();
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

	//	System.out.println(starToCS.keySet().toString());
		
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
		//	System.out.println("Center is: "+center+"\t"+starToCS.keySet().toString());
		}
				
		ArrayList<HashMap<String, Set<String>>> partialResult = new ArrayList();

		for(Set<String> centerCS: starToCS.get(center)){
			
			HashMap<String, Set<String>> map = new HashMap();
			map.put(center, centerCS);
			partialResult.add(map);
		}
		
		ArrayList<HashMap<String, Set<String>>> result = cartesianProd(center, partialResult, starToCS, starList.get(center));
		
	//	System.out.println("Total valid pairs: "+result.size());
		
		int flag=0;
		
		
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
		
	//	System.out.println("@@@ Result is: "+result.size()+"\t set size: "+set.size());
		
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

				for(String pred: outgoingEdges){

		//			localEstimate= localEstimate * (( (float)cs.get(starcs).get(pred)));

					
				//	localEstimate= localEstimate * (( (float)CS.get(starcs).get(pred)) / ((float)cs_distinct.get(starcs)));

								localEstimate= localEstimate * (( (float)cs.get(starcs).get(pred).get_count()) / ((float)cs_distinct.get(starcs)));
				}
				
			}
			
			estimatedSize= estimatedSize + localEstimate;
		
		}

	//	System.out.println("Estimated Size: "+estimatedSize);
		
	//	bw.write(query_id+"\t"+estimatedSize+"\n");
	//	bw.flush();
		return estimatedSize;
	}

	/*
	 * Difference b/w this and Estimate1: This one is handling OO join, that is query patterns with multiple starting points
	 * This solution is temporary, we need to handle it in more formal way
	 * The estiamte is calculated by adding up the estimate for each starting point
	 */
	static Float estimate2(HashMap<String,  ArrayList<String>> queryCSPairs, HashMap<String,Star> starList) throws IOException, ClassNotFoundException{
		
		
		
		HashMap<String, ArrayList<Set<String>>> starToCS =new HashMap();
		
		/*
		 * find the matching(superset of query CS) CS of dataset
		 */
		for(String starSub: starList.keySet()){
			
			String starCS = starList.get(starSub).get_CS();
			String[] cs_comp= starCS.substring(1, starCS.length()-1).split(", ");
			HashSet<String> cs_set=new HashSet(Arrays.asList(cs_comp));
			int distCount=0;

			
			ArrayList<Set<String>> csList= new ArrayList();
		
			for(Set<String> cs_id: cs.keySet()){ // for yago
						
					if(cs_id.containsAll(cs_set)){
			
					csList.add(cs_id);
				}
				
			}
			
			starToCS.put(starSub, csList);
		//	System.out.println("Star: "+starSub);


		}

	//	System.out.println("********Star Result:");
	//	System.out.println(starToCS.keySet().toString());
		
	//	String  center= new String();
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
		
		
					

		
		// Difference
		ArrayList<Float> estimatedSizeList = new ArrayList();
		
	//	System.out.println("Number of starting points: "+centerSet.size());
	
		if(centerSet.size()==1){
			
			String center=centerSet.iterator().next();
		//	System.out.println("Center is: "+center+"\n"+starToCS.values().toString());
		}


		for (String center: centerSet) {
			ArrayList<HashMap<String, Set<String>>> partialResult = new ArrayList();

			for(Set<String> centerCS: starToCS.get(center)){
			
				HashMap<String, Set<String>> map = new HashMap();
				map.put(center, centerCS);
				partialResult.add(map);
			//	System.out.println(centerCS.toString());
			}
		
		ArrayList<HashMap<String, Set<String>>> result = cartesianProd(center, partialResult, starToCS, starList.get(center));
		int flag=0;
		int c=0;
		Float estimatedSize= new Float(0.0);
		
		//difference
		HashSet<String> localStarList = new HashSet();
		
		for (HashMap<String, Set<String>> e: result) {
			localStarList.addAll(e.keySet());
		}
		//  
		HashSet set = new HashSet();
		set.addAll(result);
		
		for(HashMap<String, Set<String>> entry: result){
		
	//		System.out.println("REsult Keys: "+entry.keySet().toString());
			
			//		for(String key: entry.keySet())
			//		System.out.println(starList.get(key).get_CS()+"\t"+entry.get(key));
					
					Float localEstimate = new Float(1.0);
				//	localEstimate= ((float) cs_distinct.get(entry.get(center))); //for yago
					localEstimate= ((float) CSDistinct.get(entry.get(center)));

					HashMap<String,  HashSet<String>> redundantEdges= new HashMap();
			//		System.out.println("Query CS Pair: "+queryCSPairs.keySet().toString());

					HashSet<String> centerNodesSet = new HashSet();
					centerNodesSet.addAll(entry.keySet());
	
					for(String cspair: queryCSPairs.keySet()){
						
						String comp[]= cspair.split(" : ");
						String sub= comp[0];
						String obj= comp[1];
						
						/*
						 * it is to check if the CS pair is related to the correct startingpoint or not
						 */
						if(centerNodesSet.contains(sub) && centerNodesSet.contains(obj)) {
							
							
							Set<String> subCS= entry.get(sub);
							Set<String> objCS=entry.get(obj);
							
					//		System.out.print("CSPair: "+cspair);
					//		System.out.print("Obj: "+obj);
							
					//		System.out.print("SubCS: "+subCS.toString());
					//		System.out.print("ObjCS: "+objCS.toString());
							
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
							
							
						//	System.out.println("CS Predicates: "+csPair.get(cs_pair_id).keySet().toString());
							
							for(String pred: queryCSPairs.get(cspair)){
							
							//	System.out.println("Pred: "+ pred);
						//	System.out.println(cspair+"\t"+pred+"\t"+(((float)csPair.get(cs_pair_id).get(pred)) / ((float) csPair_distinct.get(cs_pair_id))));
							
									if (!csPair.containsKey(cs_pair_id)) {
										System.out.println("csPair at fault \t"+cs_pair_id);
									} 
									
									if (!csPair_distinct.containsKey(cs_pair_id)) {
										System.out.println("csPair_distinct at fault \t"+cs_pair_id);
									
									} else if (!csPair.get(cs_pair_id).containsKey(pred)) {

										System.out.println("csPair_distinct at fault \t"+cs_pair_id);	
									}
									
									if (!csPair.containsKey(cs_pair_id)) {
										System.out.println("csPair at fault \t"+cs_pair_id);
									} 
									
								localEstimate= localEstimate* (((float)csPair.get(cs_pair_id).get(pred)) / ((float) csPair_distinct.get(cs_pair_id)));
					
						}
							if(localEstimate==0)
								break;
							
						}
								

					}
				//	System.out.println("stars: "+starList.keySet().toString());
					
					for(String starid: localStarList){
						
						Set<String> starcs= entry.get(starid);
						Star star= starList.get(starid);
				
					//	System.out.println("Entry in resultStar :"+starcs.toString() );
						
						HashSet<String> outgoingEdges = new HashSet(star.outgoingEdgeLabels());
						if(redundantEdges.containsKey(starid))
							outgoingEdges.removeAll(redundantEdges.get(starid));

					//	System.out.println("Redunudant: "+outgoingEdges);

						for(String pred: outgoingEdges){

				//			localEstimate= localEstimate * (( (float)cs.get(starcs).get(pred)));

							
						//	localEstimate= localEstimate * (( (float)CS.get(starcs).get(pred)) / ((float)cs_distinct.get(starcs)));

					//		if (cs.get(starcs).containsKey(pred))
					//			System.out.println("\ncs doesn't exits!!"+pred+"\n");

							
						//	localEstimate= localEstimate * (( (float)cs.get(starcs).get(pred).get_count()) / ((float)cs_distinct.get(starcs))); //for yago
							localEstimate= localEstimate * (( (float)cs.get(starcs).get(pred).get_count()) / ((float)CSDistinct.get(starcs)));

					//		System.out.println("Pedicate: "+pred);

						}
						
					}
					
					estimatedSize= estimatedSize + localEstimate;
				
				}
		
		estimatedSizeList.add(estimatedSize);
	//	System.out.println("Total valid pairs: "+result.size());

	}
		
	Float estimatedSize = new Float(0.0);
	
	for (Float e: estimatedSizeList) {
		estimatedSize = estimatedSize + e;
	}
		
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
		
	//	System.out.println("@@@ Result is: "+result.size()+"\t set size: "+set.size());
		
	
	//	System.out.println("Estimated Size: "+estimatedSize);
		
	//	bw.write(query_id+"\t"+estimatedSize+"\n");
	//	bw.flush();
		return estimatedSize;
	}

	private static ArrayList<HashMap<String, Set<String>>> cartesianProd(String center,
			ArrayList<HashMap<String, Set<String>>> partialResult, HashMap<String, ArrayList<Set<String>>> starToCS, Star  star) {
		
		ArrayList<HashMap<String, Set<String>>> forwardResult = null;
		HashSet<String> neighbors= new HashSet(star.getNeighbors());
		
		neighbors.retainAll(starToCS.keySet());
		
		
		if(neighbors.size()>0){
			
			
			for(String neighbor: neighbors){
				
				forwardResult= new ArrayList();
			//	System.out.println("center: "+center+"\t neighbor: "+ neighbor);

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
				
				partialResult.removeAll(partialResult);
				
				
				if(forwardResult.size()>0)
				partialResult.addAll(cartesianProd(neighbor, forwardResult, starToCS, starList.get(neighbor)));
				else
					break;
				
			}
			
		}
		else{
			
			return partialResult;
		}
				
		
		return forwardResult;
	}


}
