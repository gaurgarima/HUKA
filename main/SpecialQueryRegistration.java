package main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.syntax.ElementPathBlock;
import org.apache.jena.sparql.syntax.ElementVisitorBase;
import org.apache.jena.sparql.syntax.ElementWalker;

import preprocessing.queryRegistry.annotator.SubQuery;
import preprocessing.queryRegistry.executionPlan.global.InternalRelation;

public class SpecialQueryRegistration {

	/*
	 * Subquey map, internal relation map
	 * predicateToAffectedQueryMap
	 * queryToComponentMap
	 * subqueryToInternalMap
	 */
	static HashMap<String, SubQuery> subqueryMap;
	static HashMap<String, InternalRelation> internalRelationMap;
	static HashMap<String, HashMap<String,String>> predicateToAffectedQueryMap;
	static HashMap<String, HashMap<String, ArrayList<String>>> queryToComponentMap;
	static HashMap<String, String> subqueryToInternalRelationMap;
	static HashMap<String, HashSet<String>> queryToSubqueryMap;
	static HashMap<String, String> predicateCorrectMap;
	static HashMap<String, String> predicateMapping;
	static HashMap<String, String> predicateReverseMap;

	static HashMap<String, String> rangeMap = new HashMap();
	static HashMap<String, String> domainMap = new HashMap();

	
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
	
	//	correct GlobalDS
		ConfigurationReader.readConfiguration();
		GlobalDSReader.instantiate();
		
		subqueryMap = new HashMap();
		subqueryMap.putAll(GlobalDSReader.getSubQueryMap());
		
		internalRelationMap = new HashMap();
		internalRelationMap.putAll(GlobalDSReader.getInternalRelationMap());
		/*
		 * Instantiate subqueryToInternalRelationMap
		 */
		buildSubqueryToIRMap();
		loadCorrectPredicateMap();
		/*
		 * 
		 */
		queryToSubqueryMap = new HashMap();
		
		buildQueryTOSubQueryMap();
		
		initializeRangeMap();
		initializeDomainMap();
		
		/*
		 * Save all the Maps
		 */
		
		File file = new File("./meta/dbpedia/query/special/subqueryToIRMap.ser"); // TODO: file name
		FileOutputStream fi1 = new FileOutputStream(file);
		ObjectOutputStream si1 = new ObjectOutputStream(fi1);
			
		si1.writeObject(subqueryToInternalRelationMap);
		si1.close();
		fi1.close();

		File file1 = new File("./meta/dbpedia/query/special/queryToSubqueryMap.ser"); // TODO: file name
		FileOutputStream fi11 = new FileOutputStream(file1);
		ObjectOutputStream si11 = new ObjectOutputStream(fi11);
			
		si11.writeObject(queryToSubqueryMap);
		si11.close();
		fi11.close();

	
		
		/*
		 * Read nmaps
		 */
		
/*		File file = new File("./meta/dbpedia/query/special/subqueryToIRMap.ser"); // TODO: file name
		FileInputStream fi1 = new FileInputStream(file);
		
		ObjectInputStream si1 = new ObjectInputStream(fi1);
		
		subqueryToInternalRelationMap = (HashMap<String, String>) si1.readObject();
		si1.close();
		fi1.close();

		
		File file1 = new File("./meta/dbpedia/query/special/queryToSubqueryMap.ser"); // TODO: file name
		FileInputStream fi11 = new FileInputStream(file1);
		ObjectInputStream si11 = new ObjectInputStream(fi11);
		
		queryToSubqueryMap = (HashMap<String, HashSet<String>>) si11.readObject();
		si11.close();
		fi11.close();

	*/	
		
		/*
		 * Identify special queries and initiate the other DS
		 */
		
		System.out.println("Maps created!!");
		String queryFile = "finalQList1.txt";
		queryFile = "queryPartition/queryList6";
		
		BufferedReader br = new BufferedReader (new FileReader (new File("./workload/"+queryFile)));
		String line = "";
		predicateToAffectedQueryMap = new HashMap();
		queryToComponentMap = new HashMap();
		
		while ((line = br.readLine()) != null) {
			
			String lineComp[] = line.split("\t");
			String queryId = lineComp[0];
			String qStmt = lineComp[2];
			
 			HashMap<String, ArrayList<HashSet<Integer>>> predicateToSpecialEdgeMap = new HashMap();
			Query query = QueryFactory.create(qStmt);

			HashMap<Integer, String> tripleMap = new HashMap();
			
			tripleMap.putAll(generateSPARQLTuples(query));
			
			System.out.println("TripleMap: \n");
			
			for (Integer i: tripleMap.keySet()) {
				System.out.println(i+"\t"+tripleMap.get(i));
			}
			
			
			predicateToSpecialEdgeMap.putAll(isSpecialQuery(tripleMap));

			
			if (predicateToSpecialEdgeMap.size() > 0) {
				
				System.out.println("Q: "+line);

				System.out.println("TripleMap: \n");
				
				for (Integer i: tripleMap.keySet()) {
					System.out.println(i+"\t"+tripleMap.get(i));
				}
				
				for (Entry<String, ArrayList<HashSet<Integer>>> entry: predicateToSpecialEdgeMap.entrySet()) {
					
					String predicate = entry.getKey();
					
					/*
					 * First build predicateToAffectedQueryMap
					 */
									
					String subqueryId = findSubquery(queryId, predicate); // This method will find either the connected or isolated subquery
			
					if (predicateToAffectedQueryMap.containsKey(predicate)) {
						
						predicateToAffectedQueryMap.get(predicate).put(queryId, subqueryId);
					} else {
						HashMap<String, String> temp = new HashMap();
						temp.put(queryId, subqueryId);
						predicateToAffectedQueryMap.put(predicate, temp);
						
					}
					
					
					/*
					 * Build smaeEdgeMap
					 */
					
					ArrayList<String> toMatchComponents = new ArrayList();
					
					for (HashSet<Integer> sameTriples: entry.getValue()) {
						String subjects = "";
						String objects = "";
						
						for (Integer tripleId: sameTriples) {
							
							String triple = tripleMap.get(tripleId);
							String tripleComp[] = triple.split(" ");
							
							String sub = tripleComp[0];
							String obj = tripleComp[2];
							
							if (sub.startsWith("\""))
								sub = sub.replaceAll("\"", "");
							else if (sub.startsWith("<")) {
								sub = sub.replace("<", "");
								sub = sub.replace(">", "");
							}
							
							if (obj.startsWith("\""))
								obj = obj.replaceAll("\"", "");
							
							else if (obj.startsWith("<")) {
								obj = obj.replace("<", "");
								obj = obj.replace(">", "");
							}
							
							subjects = subjects.concat(sub+", ");
							objects = objects.concat(obj+", ");
						}
						
						subjects = subjects.substring(0, subjects.lastIndexOf(", "));
						objects = objects.substring(0, objects.lastIndexOf(", "));
						
						toMatchComponents.add(subjects+" : "+objects);
						
					}
					
					

					System.out.println("Qid: "+queryId+"\nPredicate: "+predicate+" -> "+toMatchComponents);
					
					if (queryToComponentMap.containsKey(queryId)) {
						queryToComponentMap.get(queryId).put(predicate, toMatchComponents);
						
						
					} else {
						HashMap<String, ArrayList<String>> temp = new HashMap();
						temp.put(predicate, toMatchComponents);
						
						queryToComponentMap.put(queryId, temp);
					}
					
					
				}
				
			}
		}
		
		System.out.println("Predicates ahve special query: "+predicateToAffectedQueryMap.size());
		System.out.println("Queryto match map: "+queryToComponentMap.size());
		br.close();
		//GlobalDSReader.close();
		
		System.out.println("\nPredicate to affected query map:");
		for (Entry<String, HashMap<String, String>> entry: predicateToAffectedQueryMap.entrySet()) {
			System.out.println("\nPredicate: "+entry.getKey());
			
			for (Entry<String, String> e: entry.getValue().entrySet()) {
				System.out.println(e.getKey()+" ---> "+e.getValue()+"\t"+subqueryMap.get(e.getValue()).getExpectedRelation());
			}
		}
		
		System.out.println("\n\nQuery to affected nodes map:");
		
		for (Entry<String, HashMap<String, ArrayList<String>>> entry: queryToComponentMap.entrySet()) {
			System.out.println("Query: "+entry.getKey());
			
			for (Entry<String, ArrayList<String>> e: entry.getValue().entrySet()) {
				System.out.println(e.getKey()+" ---> "+e.getValue().toString());
			}
		}
		
		
		File file2 = new File("./meta/dbpedia/query/special/predicateToAffectedQueryMap.ser"); // TODO: file name
		FileOutputStream fi2 = new FileOutputStream(file2);
		ObjectOutputStream si2 = new ObjectOutputStream(fi2);
			
		si2.writeObject(predicateToAffectedQueryMap);
		si2.close();
		fi2.close();

		File file3 = new File("./meta/dbpedia/query/special/queryToComponentMap.ser"); // TODO: file name
		FileOutputStream fi3 = new FileOutputStream(file3);
		ObjectOutputStream si3 = new ObjectOutputStream(fi3);
			
		si3.writeObject(queryToComponentMap);
		si3.close();
		fi3.close();
		
		File file4 = new File("./meta/dbpedia/query/special/rangeMap.ser"); // TODO: file name
		FileOutputStream fi4 = new FileOutputStream(file4);
		ObjectOutputStream si4 = new ObjectOutputStream(fi4);
			
		si4.writeObject(rangeMap);
		si4.close();
		fi4.close();
		
		File file5 = new File("./meta/dbpedia/query/special/domainMap.ser"); // TODO: file name
		FileOutputStream fi5 = new FileOutputStream(file5);
		ObjectOutputStream si5 = new ObjectOutputStream(fi5);
			
		si5.writeObject(domainMap);
		si5.close();
		fi5.close();
		
	}
	
	private static void loadCorrectPredicateMap() throws IOException {

		BufferedReader br = new BufferedReader(new FileReader(new File("./meta/dbpedia/kg/raw/sqlTableName.txt")));
		
		String l = "";
		predicateMapping = new HashMap();
		predicateCorrectMap = new HashMap();
		predicateReverseMap = new HashMap();
		
		while ((l = br.readLine()) != null) {
			String lComp[] = l.split("\t");
			predicateMapping.put(lComp[0].trim().toLowerCase(), lComp[1].trim());
			//predicateCorrectMap.put(lComp[0].trim().toLowerCase(), lComp[0]);
			predicateReverseMap.put(lComp[1].trim(), lComp[0].trim());

		}
		
		br.close();
		
	}

	private static String findSubquery(String queryId, String predicate) {
	
		
		HashSet<String> subqueryIds = new HashSet();
		String subqueryId = "";
		subqueryIds.addAll(queryToSubqueryMap.get(queryId));
		boolean hasFound = false;
		
		for (String id: subqueryIds) {
			
			SubQuery squery=  subqueryMap.get(id);
			
			if (!squery.getExpectedRelation().equals(predicate)) {
				
				String cp1Variable = squery.getCP1Variable();
				String cp2Variable = squery.getCP1Variable();
				String counterQ = squery.getCounterQuery();
				
				if (counterQ.equals("NULL")) {
				
					hasFound = true;
					subqueryId = id;
					break;
				}
			}
			
		}
		
		if (!hasFound) {
		
			for (String id: subqueryIds) {
				
				SubQuery squery=  subqueryMap.get(id);
					
				String cp1Variable = squery.getCP1Variable();
				String cp2Variable = squery.getCP1Variable();
				String counterQ = squery.getCounterQuery();
					
				if (counterQ.equals("NULL")) {
				
					hasFound = true;
					subqueryId = id;
					break;
				}
				
			}
		}
		
		
		return subqueryId;
	}

	private static void buildQueryTOSubQueryMap() {
		
		queryToSubqueryMap = new HashMap();
		
		for (Entry<String, SubQuery> entry: subqueryMap.entrySet()) {
			
			String parentQueryId = entry.getValue().getParentQueryId();
			
			if (queryToSubqueryMap.containsKey(parentQueryId)) {
				queryToSubqueryMap.get(parentQueryId).add(entry.getKey());
			
			} else {
				HashSet<String> temp = new HashSet();
				temp.add(entry.getKey());
				queryToSubqueryMap.put(parentQueryId, temp);
			}
			
		}
		
	}

	private static void buildSubqueryToIRMap() {
		
		for (Entry<String, InternalRelation> entry: internalRelationMap.entrySet()) {
			
			if (entry.getValue().isRoot()) {
			
				HashSet<String> subqueryId = new HashSet();
				subqueryId.addAll(entry.getValue().getQVarMap().keySet());
				subqueryToInternalRelationMap = new HashMap();
			
				for (String sId: subqueryId) {
					
					subqueryToInternalRelationMap.put(sId, entry.getKey());
				}
			}
		}
		
	}

	private static HashMap<Integer, String> generateSPARQLTuples(Query query) {
		HashMap<Integer, String> map =new HashMap();
		int i = 0;
		ArrayList<String> tripleList= new ArrayList();
		
		ElementWalker.walk(query.getQueryPattern(),
			    // For each element...
			    new ElementVisitorBase() {
			        // ...when it's a block of triples...
			        public void visit(ElementPathBlock el) {
			            // ...go through all the triples...
			            Iterator<TriplePath> triples = el.patternElts();
			            
			            while (triples.hasNext()) {
			                // ...and grab the subject
			            	//subjects.add(triples.next().getSubject());
			            	
			            	TriplePath triple= new TriplePath(triples.next().asTriple());
			            	//triple=triples.next();
			            	//tripleList.add(triple.toString());
			            	
			            	
			            	String subject = triple.getSubject().toString();
			            	String object = triple.getObject().toString();
			            	String pred = triple.getPredicate().toString();
			            	
			           // 	System.out.println(subject);
			          //  	System.out.println(pred);
			          //  	System.out.println(object);
		         	
			 
			            	
			       /*     	if (triple.getSubject().isURI())
			            		subject = "<"+subject+">";
			            	
			            	if (triple.getObject().isURI())
			            		object = "<"+object+">";
			         */   	
			         //   	pred = predicateCorrectMap.get("<"+pred.toLowerCase()+">");
			            	
			            	
			            	
			               	if (subject.contains("dbpedia-owl:") || subject.equals("rdfs:label")) {
			            		String predComp[] = subject.split(":");
			            		
			            		if (predComp[0].trim().equals("dbpedia-owl"))
			            			subject = "http://dbpedia.org/ontology/"+predComp[1].trim();
			            		else
			            			subject = "http://www.w3.org/2000/01/rdf-schema#"+predComp[1].trim();
			            		
			            	}
			            	if (object.contains("dbpedia-owl:") || object.equals("rdfs:label")) {
			            		String predComp[] = object.split(":");
			            		
			            		if (predComp[0].trim().equals("dbpedia-owl"))
			            			object = "http://dbpedia.org/ontology/"+predComp[1].trim();
			            		else
			            			object = "http://www.w3.org/2000/01/rdf-schema#"+predComp[1].trim();
			            		
			            	}
			            	
			            	if (triple.getObject().isLiteral())
			            		object = triple.getObject().getLiteralValue().toString();
			            
			            	
			            //	System.out.println(triple.getSubject().toString()+"\n"+triple.getSubject().getIndexingValue().toString());
			            	/*
			            	if (object.contains("^^")) {
			            		object = object.substring(0, object.indexOf("^^"));
			            	}*/
			            	
			            	
			            	String smallCasePred = "";
			            	
			            	if (pred.contains("dbpedia-owl:") || pred.equals("rdfs:label")) {
			            		String predComp[] = pred.split(":");
			            		
			            		if (predComp[0].trim().equals("dbpedia-owl"))
			            		pred = "http://dbpedia.org/ontology/"+predComp[1].trim();
			            		else
			            			pred = "http://www.w3.org/2000/01/rdf-schema#"+predComp[1].trim();
			            		
			            	} 			
			            	
			            	if (pred.contains("rdf:type")) {
			            		
			            		pred = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
			            	}
		            		pred = pred.toLowerCase();

			         //   	System.out.println(pred);
			            	
			            	
			           // 	System.out.println(pred);
			            	if (predicateMapping.containsKey("<"+pred.toLowerCase()+">")) {
			            		
			            		String triple_stmt = subject+" "+predicateMapping.get("<"+pred.toLowerCase()+">")+" "+object;
			            //		System.out.println(triple_stmt);
			            		tripleList.add(triple_stmt);
			            	
			            	} 

			            	
			            	
			            	
			            	
			            	/*if (!pred.startsWith("?"))
			            		pred = "<"+pred+">";
			            	*/
			            	
			            	
			 /*           	if (!subject.startsWith("?")) {
			            		
			            		int indexOfLastSLash = subject.lastIndexOf("/");
			            		subject = subject.substring(indexOfLastSLash + 1, subject.length());
			            		
			            	}
			            	
			            	if (!object.startsWith("?")) {
			            		
			            		int indexOfLastSLash = object.lastIndexOf("/");
			            		object = object.substring(indexOfLastSLash + 1, object.length());
			            		
			            	}
			            	
			            	if(!pred.startsWith("?")) {
			            		String relation_comp[] = pred.split("/");
			        			pred = relation_comp[relation_comp.length-1];
			        			
			        			if (pred.contains("#")) {
			        				int pos = pred.indexOf("#");
			        				pred = pred.substring(pos+1, pred.length());
			        			}    
			        			else
			        				pred = pred.substring(0,pred.length());
			        		
			            	}
			   */         
			            //	String triple_stmt = subject+" "+pred+" "+object;
			            //	tripleList.add(triple_stmt);

			            	
			            	
			           /* 	System.out.println(triple.toString());
			            	String[] cop = triple.toString().split(" ");
			            	System.out.println("Size: "+cop.length);
			            	System.out.println(triple.getSubject().toString());
			            	System.out.println(triple.getPredicate());
			            	System.out.println(triple.getObject());
			            	System.out.print("\n\n");
			            */
			            }
			        }
			    }
			);
		
		for (String str: tripleList) {
		//	System.out.println(i+"\t"+str);
			map.put(i, str);
			i++;
		}
		
	//	System.out.println("\n");

		return map;
	}
	
	
	private static HashMap<String, ArrayList<HashSet<Integer>>> isSpecialQuery (HashMap<Integer, String> tripleMap2) {
		
		boolean isSpecial = false;
		HashMap<Integer, String> tripleMap = new HashMap();
		HashMap<String, HashSet<Integer>> samePredicateTripleMap = new HashMap();

		// Step1: Associate each triple of the query to an ID
	//	tripleMap.putAll(generateSPARQLTuples(tripleMap2));
		tripleMap.putAll(tripleMap2);
		
		int tripleCount = tripleMap.size();
		
		String[][] neighborMap = new String[tripleCount][tripleCount];
		boolean hasBoundComponent = false;
		
		// Step 2: Generate an adjacency list for the Graph where each node is a qeury triple; Also collect triples with same predicates together
		
		for (Entry<Integer, String> entry: tripleMap.entrySet()) {
			
			Integer id1 = entry.getKey();
			String triple1 = entry.getValue();
			String triple1Comp[] = triple1.split(" ");
			
			if (!triple1Comp[0].startsWith("?") || !triple1Comp[2].startsWith("?"))
				hasBoundComponent = true;
			
			for (Integer id2 : tripleMap.keySet()) {
				
				if (!id2.equals(id1)) {
					
					String triple2 = tripleMap.get(id2);
					String triple2Comp[] = triple2.split(" ");
					
					// check the join type -- SS, OS, SO, OO
					if (triple2Comp[0].equals(triple1Comp[0])) {
						
						neighborMap[id1][id2] = "SS";
						neighborMap[id2][id1] = "SS";

					} else if (triple2Comp[0].equals(triple1Comp[2])) {

						neighborMap[id1][id2] = "OS";
						neighborMap[id2][id1] = "SO";

						
					} else if (triple2Comp[2].equals(triple1Comp[0])) {
						neighborMap[id1][id2] = "SO";
						neighborMap[id2][id1] = "OS";
						
						
					} else if (triple2Comp[2].equals(triple1Comp[2])) {
						neighborMap[id1][id2] = "OO";
						neighborMap[id2][id1] = "OO";
						
					} else {
						neighborMap[id1][id2] = "-";
						neighborMap[id2][id1] = "-";
					}
					
					
					if (triple1Comp[1].equals(triple2Comp[1])) {
						
						if (samePredicateTripleMap.containsKey(triple1Comp[1])) {
							samePredicateTripleMap.get(triple1Comp[1]).add(id1);
							samePredicateTripleMap.get(triple1Comp[1]).add(id2);
							
						//	System.out.println(triple1Comp[1]+" -> "+ id1+", "+id2);
						
						} else {
						
							HashSet<Integer> idList = new HashSet();
							idList.add(id1);
							idList.add(id2);

							samePredicateTripleMap.put(triple1Comp[1], idList);
						}
					}
					
				} else {
					neighborMap[id1][id2] = "-";
				}
				
			}
		}
		
		
	/*	for (Integer i: tripleMap2.keySet()) {
			String neighbor = "";
			
			for (int j = 0; j < tripleMap2.size(); j++) {
				neighbor = neighbor.concat(j+":"+neighborMap[i][j]+"\t");
			}
			
			System.out.println(i+"\t"+neighbor);
			
		}
		*/
			
		// Step3:  Find if the query could generate 1:M potential match by analyzing each predicate
		
		HashMap<String, ArrayList<HashSet<Integer>>> predicateToSpecialEdgeMap = new HashMap();
		
		for (Entry<String, HashSet<Integer>> entry: samePredicateTripleMap.entrySet()) {
			
			String predicate = entry.getKey();
			
			if (entry.getValue().size() > 1) {
				
				HashMap<Integer, ArrayList<String>> subjectSubgraphMap = new HashMap();
				
				ArrayList<Integer> sameTriple = new ArrayList();
				
				
				sameTriple.addAll(entry.getValue());
				
				Iterator it = entry.getValue().iterator();
				
				//Step 3.1: For each concerned predicate, find the subtree for each associated triple; subtrees are rooted at the subject of the triple
				
				while (it.hasNext()) {
					
					Integer involvedTripleId = (Integer) it.next();
					sameTriple.remove(involvedTripleId);
					
				//	System.out.println("SQ: "+involvedTripleId+"\t"+sameTriple.toString());
					subjectSubgraphMap.put(involvedTripleId, findSubgraph(involvedTripleId, sameTriple, true, neighborMap, tripleMap));
					sameTriple.add(involvedTripleId);	
					
				/*	System.out.println(involvedTripleId+" : paths: ");
					for (String p: subjectSubgraphMap.get(involvedTripleId)) {
						System.out.println(p);
					}
					*/
					
				}
				
				
					
									

				//Step 3.3: If the query has bound components, then further check
				if (hasBoundComponent) {
					
					System.out.println("Has bounded variables");
					
					HashMap<Integer, HashMap<String, ArrayList<String>>> predicatePaths = new HashMap();
					HashMap<Integer, ArrayList<String>> pathToOtherSubject = new HashMap();
					
					// Step 3.3.1: Convert each path into predicates and segregate them based on if they have bound component 
					for (Entry<Integer, ArrayList<String>> entry1: subjectSubgraphMap.entrySet()) {
						
						HashMap<String, ArrayList<String>> temp  =new HashMap();
						System.out.println("\nPP of "+entry1.getKey()+" --");
						
						ArrayList<String> paths = new ArrayList();
						paths.addAll(entry1.getValue());
						
						if (! tripleMap.get(entry1.getKey()).split(" ")[2].startsWith("?"))
							paths.add(entry1.getKey().toString());
						
						HashSet<Integer> sameTripleTemp = new HashSet();
						sameTripleTemp.addAll(sameTriple);
						sameTripleTemp.remove(entry1.getKey());
						
						for (String triplePath: paths) {
							
							String pathComp[] = triplePath.split("; ");
							Integer lastTriple = Integer.parseInt(pathComp[pathComp.length-1]);
							String path = "";

							for (String str : pathComp) {
								if (str.length() > 0) {
									String triple = tripleMap.get(Integer.parseInt(str));
									path = path.concat(triple.split(" ")[1]+"; ");
								}
							}
							
							path =path.substring(0, path.lastIndexOf("; "));
						
							System.out.println("raw: "+path);
							
							if (sameTripleTemp.contains(lastTriple)) {
								
								if (path.contains(";")) { 
									path = path.substring(0, path.lastIndexOf("; "));
								
									if (pathToOtherSubject.containsKey(entry1.getKey())) {
										
										pathToOtherSubject.get(entry1.getKey()).add(path+"; "+lastTriple);
									} else {
									
										ArrayList<String> l2 = new ArrayList();
										l2.add(path+"; "+lastTriple);
										pathToOtherSubject.put(entry1.getKey(), l2);
									}
									
									System.out.println("PathToOtherSubject:  --> "+pathToOtherSubject.get(entry1.getKey()));
								}
								
							} else {
								
								String t = tripleMap.get(lastTriple);
								
									
								
								if (!t.split(" ")[2].startsWith("?")) { // object is bounded
									
									path = path.concat("; const : "+t.split(" ")[2]);
									
									if (temp.containsKey("BO")) { //bounded path
									
										temp.get("BO").add(path);
									} else {
										
										ArrayList<String> t1 = new ArrayList();
										t1.add(path);
										temp.put("BO", t1);
									}
									
								} else { //path is unbounded
									
									if (temp.containsKey("UB")) { //bounded path
										
										temp.get("UB").add(path);
									} else {
										
										ArrayList<String> t1 = new ArrayList();
										t1.add(path);
										temp.put("UB", t1);
									}
								}
								
							}
							
							if (temp.containsKey("BO"))
								System.out.println("BO:  ->  "+temp.get("BO").toString());
							
							
						}
						
						//if (temp.size() > 0)
							predicatePaths.put(entry1.getKey(), temp);
					}
					
					
					
					// Step 3.2: Check if the concerend subjects are connected via valid path
					// TODO: check the path between concerned triples (done)
					HashMap<Integer, HashSet<Integer>> nonMatchingSubjects = new HashMap();
					
					for (Entry<Integer, ArrayList<String>> e: pathToOtherSubject.entrySet()) {
						
						for (String path: e.getValue()) {
							
							String pathComp[] = path.split("; ");
							Integer otherTripleId = Integer.parseInt(pathComp[pathComp.length-1]);
							
							String pathToCheck = path.substring(0, path.lastIndexOf("; "));
							
							if (!isValidPath(pathToCheck)) {
								
								if (nonMatchingSubjects.containsKey(e.getKey())) {
									nonMatchingSubjects.get(e.getKey()).add(otherTripleId);
								} else {
									
									HashSet<Integer> t = new HashSet();
									t.add(otherTripleId);
									nonMatchingSubjects.put(e.getKey(), t);
								}
								
								if (nonMatchingSubjects.containsKey(otherTripleId)) {
									nonMatchingSubjects.get(otherTripleId).add(e.getKey());
								} else {
									
									HashSet<Integer> t = new HashSet();
									t.add(e.getKey());
									nonMatchingSubjects.put(otherTripleId, t);
								}
								
								
							}
							
						}
						
					}
				
					
					// first chek path to other subjects and then check bounded (BO) path of each subject
					
					//Step 3.3.2 For each triple find which triples it matches to by checking their bound component paths of subtree
					
					HashSet<Integer> concernedTriples  =new HashSet();
					concernedTriples.addAll(predicatePaths.keySet());
					LinkedHashMap<Integer, HashSet<Integer>> sameTriples = new LinkedHashMap();
					
					for (Entry<Integer, HashMap<String, ArrayList<String>>> entry2 : predicatePaths.entrySet()) {
					
						concernedTriples.remove(entry2.getKey());
						
						HashSet<Integer> temp = new HashSet(concernedTriples);
						
						if (nonMatchingSubjects.containsKey(entry2.getKey())) {
							System.out.println(entry2.getKey()+" doesn't match "+nonMatchingSubjects.get(entry2.getKey()).toString());
							temp.removeAll(nonMatchingSubjects.get(entry2.getKey()));
						}
				
						
						if (!entry2.getValue().containsKey("BO") && temp.size() > 0) {
							
								
							sameTriples.put(entry2.getKey(), temp);
							//concernedTriples.add(entry2.getKey());
						} else {
							
//							for (String boundPath: entry2.getValue().get("BO")) { 
								
	//							String toFindPath = boundPath.substring(0, boundPath.lastIndexOf("; "));
								
								for (Integer otherSubject: temp) {
									
									boolean isSame = true;
									
									if (predicatePaths.get(otherSubject).containsKey("BO")) {
										
										for (String boundPath: entry2.getValue().get("BO")) { 
											String toFindPath = boundPath.substring(0, boundPath.lastIndexOf("; "));

											for (String p: predicatePaths.get(otherSubject).get("BO")) {
											
												if (p.startsWith(toFindPath)) {
												
													if (boundPath.split("; ").length == p.split("; ").length) {
													
														if (!boundPath.equals(p)) {
															isSame = false;
															break;
														}
													}
												}
											}
											
										}
										
									}
									
									if (isSame) {
										
										if (sameTriples.containsKey(entry2.getKey())) {
											sameTriples.get(entry2.getKey()).add(otherSubject);
										} else {
											
											HashSet<Integer> t3 = new HashSet();
											t3.add(otherSubject);
											sameTriples.put(entry2.getKey(), t3);											
										}
										
									}
								
							}
							
						}
							
					} // found out which triple is compatible with which
					
					//make pairs of correct triples
					
					System.out.println("Same triple groups per subject: ");
					
					for (Integer key: sameTriples.keySet()) {
						System.out.println("Sub "+key+" : "+sameTriples.get(key));
					}
					
				//	Correct the partitioning; the order of partitions matters
					
				//Step 3.2.3 Form groups of triples which matches to each other
					
					LinkedHashMap<Integer,HashSet<Integer>> sameTripleGroups = new LinkedHashMap();

					if (sameTriple.size() > 1) {
						
						boolean firstTriple = true;
						Integer firstTripleId = 0;
						sameTripleGroups.putAll(sameTriples);
						
						for (Entry<Integer, HashSet<Integer>> entry3: sameTriples.entrySet()) {
						
							HashSet<Integer> otherSubjects = new HashSet();
							otherSubjects.addAll(sameTripleGroups.keySet());
							otherSubjects.remove(entry3.getKey());
							
							HashSet<Integer> sameTripleIds = new HashSet();
							sameTripleIds.addAll(entry3.getValue());
							boolean hasFoundMatch = false;
							
							for (Integer otherTripleId: otherSubjects) {
								
								HashSet<Integer> otherSameTriple = new HashSet();
								otherSameTriple.addAll(sameTriples.get(otherTripleId));
								otherSameTriple.add(otherTripleId);
								
								
								HashSet<Integer> temp = new HashSet();
								temp.addAll(sameTripleIds);
								temp.add(entry3.getKey());
								
								temp.removeAll(otherSameTriple);
								
								if (temp.size() > 0) {
									
								} else {
									sameTripleGroups.remove(entry3.getKey());
									
									System.out.println("Removing "+entry3.getKey()+" entry due to "+otherSameTriple);
									hasFoundMatch = true;
									break;
								}
								
							}
								
						} // checked all the same Triples entry
						
						
						
					} else {
						
						sameTripleGroups.putAll(sameTriples);

					}
					
					//make groups of triples which can match to a same edge 
					
					for (Entry<Integer, HashSet<Integer>> entry5: sameTripleGroups.entrySet()) {
	
//					for (Entry<Integer, HashSet<Integer>> entry5: sameTriples.entrySet()) {
					//	entry5.getValue().add(entry5.getKey());
						HashSet<Integer> t = new HashSet();
						t.addAll(entry5.getValue());
						t.add(entry5.getKey());
						
						System.out.println("Same group: "+ entry5.getKey()+" --> "+t.toString());

						if (predicateToSpecialEdgeMap.containsKey(predicate)) {
							predicateToSpecialEdgeMap.get(predicate).add(t);
							//predicateToSpecialEdgeMap.get(predicate).add(entry5.getKey());
						} else {
							
							ArrayList<HashSet<Integer>> temp = new ArrayList();
							temp.add(t);
							predicateToSpecialEdgeMap.put(predicate, temp);
						}
					}
					
				}
			} // considering predicates which have more than 1 triple associated to it
					
			
		} // Consider all the the predicates in the query
		
		if (predicateToSpecialEdgeMap.size() > 0) {
			isSpecial = true;
			
			for (Entry<String, ArrayList<HashSet<Integer>>> entry: predicateToSpecialEdgeMap.entrySet()) {
				
				String partitions = "";
				
				for (HashSet<Integer> set: entry.getValue()) {
					partitions = partitions.concat(set.toString()+"\t");
				}
				
				System.out.println(entry.getKey()+" -> "+partitions);
				
			}
			
			
		} else {
			//System.out.println("Predicate Map is empty!");
		}
		
		
	//	return isSpecial;
		return predicateToSpecialEdgeMap;
		
	}


	private static boolean isValidPath(String pathToCheck) {

		boolean isValidPath = false;
		String predicates[]  = pathToCheck.split("; ");
				
				//TODO: Intitalize the maps
		if (predicates.length == 1)
			return isValidPath;
		
		for (String predicate: predicates) {
			
			System.out.println("Pred: "+predicate);

			predicate = predicateReverseMap.get(predicate);

			System.out.println("Pred: "+predicate);
			
			predicate = predicate.replace("/property/", "/ontology/");
			
		//	System.out.println("range: "+rangeMap.get(predicate));
		//	System.out.println("domain: "+domainMap.get(predicate));
			
			if (!rangeMap.get(predicate).equals(domainMap.get(predicate)))
				isValidPath = true;
		}
		
		
		return isValidPath;
	}
	
	private static void initializeRangeMap() throws IOException {
		
		//HashMap<String, String> map = new HashMap();
		
		BufferedReader br = new BufferedReader (new FileReader (new File("/data/home/garima/datasets/dbpedia3.9/ttl/ontology/rangeMap"))); //TODO file in file name
		String line = "";
		String lineComp[];
		
		while ((line = br.readLine()) != null) {
		
			lineComp = line.split(" ");
			rangeMap.put(lineComp[0], lineComp[1]);
		}
		br.close();
		
	}
	
	private static void initializeDomainMap() throws IOException {
		
	//	HashMap<String, String> map = new HashMap();
		
		BufferedReader br = new BufferedReader (new FileReader (new File("/data/home/garima/datasets/dbpedia3.9/ttl/ontology/domainMap"))); //TODO fill in file name
		String line = "";
		String lineComp[];
		
		while ((line = br.readLine()) != null) {
		
			lineComp = line.split(" ");
			domainMap.put(lineComp[0], lineComp[1]);
		}
		br.close();
		
	}


	private static ArrayList<String> findSubgraph(Integer tripleInvolved, ArrayList<Integer> secondTriple, boolean isFirst
			, String[][] neighborMap, HashMap<Integer, String> tripleMap) {

		boolean hasNeighbors = false;
		ArrayList<String> paths = new ArrayList();
		String predicate = tripleMap.get(tripleInvolved).split(" ")[1];
		
		for (int i = 0; i < neighborMap.length; i++) {
			
			String joinType = neighborMap[tripleInvolved][i];
		//	System.out.println("Join type: "+joinType);
			
		//	System.out.println("secondTripleSize: "+secondTriple.size());
			Integer k = new Integer(i);

			if (isFirst) {
				
			//	i = new Integer (i);
				
				ArrayList<String> tempPaths = new ArrayList();
				
					
				if (joinType.equals("SS") && !secondTriple.contains(k)) {
				//	System.out.println("Condition true");
					hasNeighbors = true;
					
					paths.addAll(findSubgraph(i, secondTriple, false, neighborMap, tripleMap));
				
				} else if (joinType.equals("OS") && !secondTriple.contains(k)) {
					
					hasNeighbors = true;
					
					tempPaths.addAll(findSubgraph(i, secondTriple, false, neighborMap, tripleMap));
					
					for (String str: tempPaths) {
						
						str = tripleInvolved +"; "+str;
						paths.add(str);
					}
				} else if (secondTriple.contains(k)) {
					hasNeighbors = true;
					
					if (joinType.equals("SS")) {
						hasNeighbors = true;
						paths.add("; "+i);

					} else if (joinType.equals("OS")) {
						hasNeighbors = true;
						paths.add(tripleInvolved+"; "+i);

					}
					
									}
			} else {
				
				if (joinType.equals("OS") && !secondTriple.contains(k)) {
					
					hasNeighbors = true;
					paths.addAll(findSubgraph(i, secondTriple, false, neighborMap, tripleMap));
					
				} else if (joinType.equals("OS") && secondTriple.contains(k)) {
					hasNeighbors = true;
					paths.add(((Integer)i).toString());
				}
				
			}
		}// checked all the neighbors		
		
		if (!hasNeighbors) {
			//System.out.println("we are adding some paths");
			
			paths.add(tripleInvolved.toString());
			
		} else if (!isFirst) {
			
			ArrayList<String> tempPaths = new ArrayList();
			tempPaths.addAll(paths);
			
			for (String str: tempPaths) {
				
				paths.remove(str);
				
				str = tripleInvolved+"; "+str;
				
				paths.add(str);
				
			}
			
		}
		
		return paths;
	}


}
