package preprocessing.queryRegistry.executionPlan.local;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.syntax.ElementPathBlock;
import org.apache.jena.sparql.syntax.ElementVisitorBase;
import org.apache.jena.sparql.syntax.ElementWalker;

import main.ConfigurationReader;
import main.GlobalDSReader;
import preprocessing.queryRegistry.executionPlan.global.BaseRelation;
import preprocessing.queryRegistry.executionPlan.global.InternalRelation;
import preprocessing.queryRegistry.executionPlan.global.TripleStoreHandler;

public class GlobalExecutionPlanBuilder {

	static ArrayList<AndOrGraph> andOrGraphList;
	static String dataset; 
	static int startENodeId;
	static int startOpNodeId;
	static boolean isGlobalTreeExist;
	static HashMap<String, String> predicateMapping;
	
	public GlobalExecutionPlanBuilder(boolean isGlobalTreeExist) throws IOException {
		
		this.isGlobalTreeExist = isGlobalTreeExist;
		
		if (isGlobalTreeExist) {
		
			AndOrGraph graph = GlobalDSReader.getGlobalGraph();
			ArrayList<Integer> eNodeIds = new ArrayList();
			ArrayList<Integer> opNodeIds = new ArrayList();
		
			eNodeIds.addAll(graph.getENodeList().keySet());
			opNodeIds.addAll(graph.getOpNodeList().keySet());
			Collections.sort(eNodeIds);
			Collections.sort(opNodeIds);
			startENodeId = eNodeIds.get(eNodeIds.size() - 1) + 1;
			startOpNodeId = opNodeIds.get(opNodeIds.size() - 1) + 1;
		
		}else{

			startENodeId = 1;
			startOpNodeId  = 1;
		}
		
		
		BufferedReader br = new BufferedReader(new FileReader(new File("./data/sqlTableName.txt")));
		String l = "";
		predicateMapping = new HashMap();
		
		while ((l = br.readLine()) != null) {

			String lComp[] = l.split("\t");
			predicateMapping.put(lComp[0].trim().toLowerCase(), lComp[1].trim());
		}
		
		br.close();
	}
		
	
	public static void buildPlan(HashMap<String, String> graphList) throws IOException, ClassNotFoundException, CloneNotSupportedException {
		
		dataset = ConfigurationReader.get("DATASET");
		andOrGraphList = new ArrayList();		
		HashMap<String, String> queryMap = new HashMap();
		
		queryMap.putAll(graphList);
		buildGraphWithMap (queryMap);
			
		AndOrGraphMerger merger = new AndOrGraphMerger(andOrGraphList);
		AndOrGraph globalGraph = new AndOrGraph();
		 
		 if(isGlobalTreeExist) {
			 globalGraph = merger.buildGlobalGraph(GlobalDSReader.getGlobalGraph());

		 } else {
			 globalGraph = merger.buildGlobalGraph();
		 }
		 
		TripleStoreHandler triplestorehandler = new TripleStoreHandler(globalGraph, false, dataset);
		
		GlobalDSReader.updateBaseRelationMap(triplestorehandler.getBaseRelationMap());
		GlobalDSReader.updateInternalRelationMap(triplestorehandler.getInternalRelationMap());
		GlobalDSReader.updateGlobalGraph(globalGraph);

	}
	
	
	private static void buildGraphWithMap(HashMap<String, String> queryList) throws CloneNotSupportedException {
	
		for (Entry<String,String> entry: queryList.entrySet()) {
			
			String query_stmt = entry.getValue();
			String qId = entry.getKey();
						
			Query query = QueryFactory.create(query_stmt);
			HashMap<Integer, String> map = new HashMap();
			map.putAll(generateTuples(query));
			
			HashSet<String> toProject = new HashSet();
			toProject.addAll(getProjection(query_stmt));
			
			if (toProject.size() == 1 && toProject.iterator().next().equals("*")) {
				
				toProject.clear();
				String strComp[] = null;

				for (String str: map.values()) {
					strComp = str.split(" ");
					
					if (strComp[0].trim().startsWith("?"))
						toProject.add(strComp[0].trim());				

					if (strComp[2].trim().startsWith("?"))
						toProject.add(strComp[2].trim());	
				}	
			}

			AndOrGraphBuilder builder = new AndOrGraphBuilder(startENodeId, startOpNodeId, map, toProject, qId);
			AndOrGraph graph = builder.buildGraph();	
			startENodeId = graph.getEnodeCount() + 1 + startENodeId;
			startOpNodeId = graph.getOpNodeCount() + 1 + startOpNodeId;
			andOrGraphList.add(graph);
			
			for (OperationNode opN: graph.getOpNodeList().values()) {
				
				if (opN.getParentCount() > 1) {
					 System.out.println("Error: More than 1 PArentexist!!");
					 System.exit(0);
				}
				
				if (opN.getParentCount() < 1) {
					 System.out.println("Error: No PArentexist!!");
					 System.exit(0);
				}
			}

			for (EquivalenceNode e: graph.getENodeList().values()) {
				
				if (e.getParentList().size() > 0) {
					System.out.println("Error: PArents alreadyt exist!!");
					System.exit(0);
			 	}
			}
	
		}		
	}


	private static HashMap<Integer, String> generateTuples(Query query) {
		HashMap<Integer, String> map =new HashMap();
		int i =0;
		ArrayList<String> tripleList= new ArrayList();
		
		ElementWalker.walk(query.getQueryPattern(),
			    new ElementVisitorBase() {
			        public void visit(ElementPathBlock el) {
			            Iterator<TriplePath> triples = el.patternElts();
			            
			            while (triples.hasNext()) {

			            	TriplePath triple= new TriplePath(triples.next().asTriple());

			            	String subject = triple.getSubject().toString();
			            	String object = triple.getObject().toString();
			            	String pred = triple.getPredicate().toString();
			            	  	
			            	
			            	
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
			            	
			            	
			            	if (predicateMapping.containsKey("<"+pred+">")) {
			            		
			            		String triple_stmt = subject+" "+predicateMapping.get("<"+pred+">")+" "+object;
			            		tripleList.add(triple_stmt);
			            	} 
			            }
			        }
			    }
			);
		
		for (String str: tripleList) {
			map.put(i, str);
			i++;
		}
		
		return map;
	}

	
	private static HashSet<String> getProjection(String query_stmt) {
		
		HashSet<String> projectedVariables = new HashSet();
		String[] strComp = query_stmt.split("SELECT");
		
		if(strComp.length < 2) {
			System.out.println("Query format incorrect: Cannot find projection!!");
			System.exit(0);
		
		} else {
			
			String[] interList = strComp[1].split("WHERE");
			String[] projList = interList[0].split("\\s+");
			
			for(String var: projList) {
				
				if (var.length()>0) {
					projectedVariables.add(var.trim());
				}
			}	
		}
		
		return projectedVariables;
	}
}
