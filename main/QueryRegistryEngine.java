package main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.syntax.ElementPathBlock;
import org.apache.jena.sparql.syntax.ElementVisitorBase;
import org.apache.jena.sparql.syntax.ElementWalker;

import preprocessing.queryRegistry.annotator.CPAnnotator;
import preprocessing.queryRegistry.annotator.SubQuery;
import preprocessing.queryRegistry.executionPlan.local.GlobalExecutionPlanBuilder;
import preprocessing.queryRegistry.queryExecutor.InvertedIndexBuilder;

public class QueryRegistryEngine {

	static String dataset;
	static BufferedWriter bw;
	static String prefix;
	static HashMap<String, SubQuery> sMap;
	static HashMap<String, String> predicateMapping;
	static HashMap<String, String> predicateCorrectMap;
	

	public static void main(
			String[] args) throws IOException, SQLException, ClassNotFoundException, CloneNotSupportedException {

		ConfigurationReader.readConfiguration();
		dataset = ConfigurationReader.get("DATASET");
		BufferedReader br = new BufferedReader(new FileReader(new File (args[0]))); // File containing query list (./query/queryList.txt)
		GlobalDSReader.instantiate();
		String line = "";
		int count = 0;
		boolean isFirst = false;
		QueryRegistryEngine queryRegistor = new QueryRegistryEngine();

		while ((line = br.readLine()) != null) {					
			
			if(count == 0)
				isFirst = true;
			
			String lineComp[] = line.split("\t");
			String qStmt = lineComp[2];
			String qId = lineComp[0];
			
			queryRegistor.registerParentQuery(qId,qStmt);
			String status = queryRegistor.registerSubQuery(qStmt, qId, isFirst);
		
			count++;
			GlobalDSReader.saveMaps();
			isFirst = false;
		}

		br.close();
		GlobalDSReader.close();

	}


	public QueryRegistryEngine() throws  IOException, ClassNotFoundException {
		
		BufferedReader br = new BufferedReader(new FileReader(new File("./data/sqlTableName.txt")));
		String l = "";
		predicateMapping = new HashMap();
		predicateCorrectMap = new HashMap();
		dataset = ConfigurationReader.get("DATASET");
		
		while ((l = br.readLine()) != null) {
			String lComp[] = l.split("\t");
			predicateMapping.put(lComp[0].trim().toLowerCase(), lComp[1].trim());
			predicateCorrectMap.put(lComp[0].trim().toLowerCase(), lComp[0]);
		}
		
		br.close();
	}
	
	public void registerParentQuery(String qId, String qStmt) throws SQLException, IOException, ClassNotFoundException {
		
		HashMap<String, String> result = new HashMap<String, String>();
		NeoQueryExecutor executor = new NeoQueryExecutor(true);
		result.putAll(executor.execute(qId, qStmt, "PQ"));
	
		if (result.size()> 0) {
			
			File f = new File(ConfigurationReader.get("QUERY_RESULT_FILE")+"Q_"+qId+".txt");
			
			if (!f.exists())
				f.createNewFile();
		
			BufferedWriter bw = new BufferedWriter(new FileWriter(f));
			
			for (Entry<String, String> line: result.entrySet()) {
				bw.write(line.getKey()+"\t"+line.getValue()+"\n");
			}

			bw.close();
			InvertedIndexBuilder invertedIndexBuilder = new InvertedIndexBuilder();
			invertedIndexBuilder.buildIndex(result, qId);
		}
		
	}

	public String registerSubQuery(String qStmt, String qId, boolean isFirst) throws IOException, ClassNotFoundException, CloneNotSupportedException, SQLException {
		
		
		String stmtComp[] = qStmt.split("SELECT ")[1].split("WHERE")[0].split(" ");

		ArrayList<String> l = new ArrayList();
		
		for (String s: stmtComp) {

			if (s.length() >= 1) {
			
				s = s.trim();
				
				if(s.startsWith("?") || s.equals("*"))
					l.add(s);
			}
		}

		sMap = new HashMap();
		String parentQId = qId;	
		HashMap<String, String> subQueryCollection = new HashMap();
		
		subQueryCollection.putAll(generateSubQuery(qStmt, parentQId, l));
		
		if(subQueryCollection.isEmpty())
			return "F";
		
		HashMap<String, HashMap<String, String>> subqueryResult = new HashMap();
		NeoQueryExecutor executor = new NeoQueryExecutor(false);
		boolean hasNonZeroResult = false;
		
		for (Entry<String, String> entry: subQueryCollection.entrySet()) {

			HashMap<String, String> result = new HashMap();			
			result.putAll(executor.execute(entry.getKey(), entry.getValue(), "SQ"));
			subqueryResult.put(entry.getKey(), result);
			
			if (result.size() > 0) {
			
				hasNonZeroResult = true;
				String q_Id = entry.getKey();
				File f = new File(ConfigurationReader.get("SUBQUERY_RESULT")+"Q_"+q_Id+".txt");
			
				if (!f.exists())
					f.createNewFile();
			
				BufferedWriter bw = new BufferedWriter(new FileWriter(f));
			
				for (Entry<String, String> line: result.entrySet()) {
					bw.write(line.getKey()+"\t"+line.getValue()+"\n");
				}
				bw.close();
			}	
		}

		
		CPAnnotator annotator = new CPAnnotator(sMap);
		annotator.buildCPMap(subqueryResult);
		annotator.buildEdgeCPMap(subqueryResult);
		annotator.annotateWithNeo();

		
		//updateCPEdge(subqueryResult);
		//updateCPMap(subqueryResult);

		/*
		* Construct local execution plan and merge it to the global plan
		*/

		if (isFirst) {
			GlobalExecutionPlanBuilder planBuilder = new GlobalExecutionPlanBuilder(false);
			planBuilder.buildPlan(subQueryCollection);
		
		} else {
			
			GlobalExecutionPlanBuilder planBuilder = new GlobalExecutionPlanBuilder(true);
			planBuilder.buildPlan(subQueryCollection);
		}
		
		return "R";
		
	}	
	

	private static HashMap<String, String> generateSubQuery(
			String qStmt, String parentQId, ArrayList<String> resultVarList) throws IOException {
		
		Query query = QueryFactory.create(qStmt);
		HashMap<Integer, String> tripleMap = new HashMap();
		tripleMap.putAll(generateTuples(query));
		
		HashMap<Integer, String> sparqlTripleMap = new HashMap();
		sparqlTripleMap.putAll(generateSPARQLTuples(query));
		String strComp[] = null;
		
		if (resultVarList.size() == 1 && resultVarList.get(0).equals("*")) {
			
			resultVarList.clear();

			for (String str: sparqlTripleMap.values()) {
				
				strComp = str.split(" ");
				
				if (strComp[0].trim().startsWith("?"))
					resultVarList.add(strComp[0].trim());
				
				if (strComp[2].trim().startsWith("?"))
					resultVarList.add(strComp[2].trim());	
			}	
		}
		
		
		int tripleCount = tripleMap.size();
		int[][] adjacencyGraph = new int[tripleCount*2][tripleCount*2];
		
		HashMap<String, Integer> graphVertexMap = new HashMap();
		HashSet<Integer> constVertex = new HashSet();
		
		for (Entry<Integer, String> entry : tripleMap.entrySet()) {
			
			String triple = entry.getValue();
			String tripleComp[] = triple.split(" ");
			int sId, dId;
			
			if (graphVertexMap.containsKey(tripleComp[0])) {
				sId = graphVertexMap.get(tripleComp[0]);

			} else {
				int size = graphVertexMap.size();
				sId = size;
				graphVertexMap.put(tripleComp[0], sId);
				
				if (!tripleComp[0].startsWith("?"))
					constVertex.add(sId);
			}
			

			if (graphVertexMap.containsKey(tripleComp[2])) {
				dId = graphVertexMap.get(tripleComp[2]);
			
			} else {
				int size = graphVertexMap.size();
				dId = size;
				graphVertexMap.put(tripleComp[2], dId);
				
				if (!tripleComp[2].startsWith("?"))
					constVertex.add(dId);
			}
			
			adjacencyGraph[sId][dId] = entry.getKey();
			adjacencyGraph[dId][sId] = entry.getKey();

		}
		
			
		int count = 1;
		
		HashMap<String, LinkedList<String>> tempSubQueryList = new HashMap();
		HashMap<String, LinkedList<String>> tempNonSparqlSubQueryList = new HashMap();

		HashMap<String, String> subQueryList = new HashMap();

		for (Entry<Integer, String> entry : tripleMap.entrySet()) {
			
			/*
			 * Copy the adjacency matrix
			 * remove and edge and check connectivity
			 * 
			 */
			int[][] copyAdjMat = cloneArray(adjacencyGraph);
			
			
			String triple= entry.getValue();
			String tripleComp[] = triple.split(" ");
			String source = tripleComp[0];
			String dest = tripleComp[2];
			String rel = tripleComp[1];
			HashSet<HashSet<Integer>> connectedComp = new HashSet();
			
			int sId = graphVertexMap.get(tripleComp[0]);
			int dId = graphVertexMap.get(tripleComp[2]);
			
			copyAdjMat[sId][dId] = 0;
			copyAdjMat[dId][sId] = 0;
			connectedComp.addAll(findConnectedComp(copyAdjMat, sId, dId, graphVertexMap.size(),constVertex));
			
			if (connectedComp.size()==0) {
				return subQueryList;
			}	
			
			Iterator it = connectedComp.iterator();
		
			/*
			 * Checking the type of the subqueries generated
			 */
			String cp1Variable = "NULL", cp2Variable = "NULL", cp1Cond = "NULL", cp2Cond = "NULL", counterQ = "NULL";
			String expectedRelation = rel;
			HashSet<String> resultVar = new HashSet();
			String line = "";
			String result = new String();

			if (connectedComp.size() == 1) {
				
				HashSet connectedComponent = new HashSet();
				connectedComponent.addAll(connectedComp.iterator().next());
				
				for (String str: resultVarList) {
					
					if (connectedComponent.contains(graphVertexMap.get(str))) {
						resultVar.add(str.replace("?",""));
					}
				}
				
				if (connectedComponent.contains(dId) && connectedComponent.contains(sId)) {
					
			//		subQueryType = "CONNECTED";
					String subQId = parentQId+"-"+Integer.toString(count);
					count++;
					
					if (source.startsWith("?")) {
						cp1Variable =  source.replace("?", "");
						resultVar.add(cp1Variable);
					}	
				
					if (dest.startsWith("?")) {
						cp2Variable =  dest.replace("?", "");
						resultVar.add(cp2Variable);
					}	
										
					for (String str: resultVar) {
						result = result.concat(str+", ");
					}
					
					result = result.substring(0, result.lastIndexOf(","));
					expectedRelation = expectedRelation.concat(" _ out");
					
					 line = parentQId+"\t"+subQId+"\t"+result+"\t"+cp1Variable+"\t"+cp2Variable+"\t"+expectedRelation+"\t"+
					cp1Cond+"\t"+cp2Cond+"\t"+counterQ;
					
					SubQuery squery = new SubQuery(line, dataset);
					sMap.put(subQId,squery);
					sMap.put(subQId,squery);

					GlobalDSReader.updateSubQueryMap(subQId, squery);
					
				} else {
				//	subQueryType=  "ISOLATED";
					
					String subQId = parentQId+"-"+Integer.toString(count);
					count++;
					HashSet<Integer> component = (HashSet<Integer>) it.next();
					String direction = "";
					
					if (component.contains(sId)) {
					
						if (source.startsWith("?")) {
						cp1Variable =  source.replace("?", "");
						resultVar.add(cp1Variable);
						}	
				
						if (!dest.startsWith("?")) {
							cp2Cond =  dest;
							cp2Cond = "eq : "+cp2Cond;
						}
						direction = "out";
					} else {
						
						if(dest.startsWith("?")) {
							cp1Variable =  dest.replace("?", "");
							resultVar.add(cp1Variable);
						}	
						
						if (!source.startsWith("?")) {
							cp2Cond =  source;
							cp2Cond = "eq : "+cp2Cond;
						}
						direction = "in";
					}
										
					for (String str: resultVar) {
						result = result.concat(str+", ");
					}
					
					result = result.substring(0, result.lastIndexOf(","));
					expectedRelation = expectedRelation.concat(" _ "+direction);
					
					line = parentQId+"\t"+subQId+"\t"+result+"\t"+cp1Variable+"\t"+cp2Variable+"\t"+expectedRelation+"\t"+
					cp1Cond+"\t"+cp2Cond+"\t"+counterQ;
					
					SubQuery squery = new SubQuery(line, dataset);
					sMap.put(subQId,squery);

					GlobalDSReader.updateSubQueryMap(subQId, squery);
				}
				
				
				HashSet<Integer> conComp = new HashSet();
				conComp.addAll(connectedComponent);
				LinkedList<String> tempList = new LinkedList();
				tempList.add(result);	
			
				while (!conComp.isEmpty()) {

					ArrayList<Integer> l = new ArrayList(conComp);
					int out = l.get(0);
					conComp.remove(out);
					
					for (Integer in : conComp) {
						
						if (copyAdjMat[out][in] > 0) {
							tempList.add(sparqlTripleMap.get(copyAdjMat[out][in]));
						}
					}
				}
				
				int c = count - 1;
				
				tempSubQueryList.put(parentQId.concat("-"+c), tempList);
				
				
			} else if(connectedComp.size() == 2) {
				
				Iterator<HashSet<Integer>> itr = connectedComp.iterator();

				HashSet<Integer> connectedComponent1 = new HashSet();
				connectedComponent1.addAll(itr.next());
				HashSet<Integer> connectedComponent2 = new HashSet();
				connectedComponent2.addAll(itr.next());
				
				if (connectedComponent1.size() == 2 || connectedComponent2.size() == 2) {
					
			//		subQueryType = "TripleDB";
					String direction = new String();
					counterQ = "";
					HashSet<Integer> singleTripleComp = new HashSet();
					HashSet<Integer> largeTripleComp = new HashSet();
					
					
					if (connectedComponent1.size() == 2) {
						singleTripleComp.addAll(connectedComponent1);
						largeTripleComp.addAll(connectedComponent2);
						
					} else {
						singleTripleComp.addAll(connectedComponent2);
						largeTripleComp.addAll(connectedComponent1);
					}
					
					
					if (singleTripleComp.contains(sId)) {
					
						direction = "in";
						String subQId = parentQId+"-"+Integer.toString(count);
						count++;
						Iterator<Integer> itr1 = singleTripleComp.iterator();
						Integer i = itr1.next();
						Integer j = itr1.next();
						Integer tripleId = copyAdjMat[i][j];
						String singleTriple = tripleMap.get(tripleId);
						String[] singleTripleComp1 = singleTriple.split(" ");
						String sub = singleTripleComp1[0];
						String obj = singleTripleComp1[2];
						String tdbName = singleTripleComp1[1];
							
						if (sub.equals(source)) {

							counterQ = "G _ "+tdbName+" _ out";
								
							if (!obj.startsWith("?")) {
								cp2Cond = "eq : "+obj+" : in";
							}
						} else if (obj.equals(source)) {

							counterQ = "G _ "+tdbName+" _ in";
								
							if (!sub.startsWith("?")) {
								cp2Cond = "eq : "+sub+" : out";
							}
						}
							
						expectedRelation = expectedRelation.concat(" _ "+direction);
						
						for (String str: resultVarList) {
								
							if (largeTripleComp.contains(graphVertexMap.get(str))) {
								resultVar.add(str.replace("?", ""));
							}
						}
							
						if (dest.startsWith("?")) {

							resultVar.add(dest.replace("?", ""));
							cp1Variable = dest.replace("?","");
						}
						
						for (String str: resultVar) {
							result = result.concat(str+", ");
						}
							
						result = result.substring(0, result.lastIndexOf(","));
						
						line = parentQId+"\t"+subQId+"\t"+result+"\t"+cp1Variable+"\t"+cp2Variable+"\t"+expectedRelation+"\t"+
									cp1Cond+"\t"+cp2Cond+"\t"+counterQ;
									
						SubQuery squery = new SubQuery(line, dataset);
						sMap.put(subQId,squery);
						GlobalDSReader.updateSubQueryMap(subQId, squery);

					} else if (singleTripleComp.contains(dId)) {
						
							direction = "out";
							String subQId = parentQId+"-"+Integer.toString(count);
							count++;
							Iterator<Integer> itr1 = singleTripleComp.iterator();
							Integer i = itr1.next();
							Integer j = itr1.next();								
							Integer tripleId = copyAdjMat[i][j];
							String singleTriple = tripleMap.get(tripleId);
							String[] singleTripleComp1 = singleTriple.split(" ");
							String sub = singleTripleComp1[0];
							String obj = singleTripleComp1[2];
							String tdbName = singleTripleComp1[1];
								
							if (sub.equals(dest)) {
								counterQ = "G _ "+tdbName+" _ out";
								
								if (!obj.startsWith("?")) {
									cp2Cond = "eq : "+obj+" : in";
								}
									
							} else if (obj.equals(dest)) {
								counterQ = "G _ "+tdbName+" _ in";
									
								if (!sub.startsWith("?")) {
									cp2Cond = "eq : "+sub+" : out";
								}
							}
								
							expectedRelation = expectedRelation.concat(" _ "+direction);
							
							for (String str: resultVarList) {
								
								if (largeTripleComp.contains(graphVertexMap.get(str))) {
									resultVar.add(str.replace("?", ""));
								}
							}
								
							if (source.startsWith("?")) {
								resultVar.add(source.replace("?", ""));
								cp1Variable = source.replace("?","");
							}
							
							for (String str: resultVar) {
								result = result.concat(str+", ");
							}
								
							result = result.substring(0, result.lastIndexOf(","));
								
							 line = parentQId+"\t"+subQId+"\t"+result+"\t"+cp1Variable+"\t"+cp2Variable+"\t"+expectedRelation+"\t"+
									cp1Cond+"\t"+cp2Cond+"\t"+counterQ;
										
							SubQuery squery = new SubQuery(line, dataset);
							sMap.put(subQId,squery);
							GlobalDSReader.updateSubQueryMap(subQId, squery);

						}
					
					HashSet<Integer> conComp = new HashSet();
					conComp.addAll(largeTripleComp);
					LinkedList<String> tempList = new LinkedList();
					tempList.add(result);
					
					while (!conComp.isEmpty()) {

						ArrayList<Integer> l = new ArrayList(conComp);
						int out = l.get(0);
						conComp.remove(out);
						
						for (Integer in : conComp) {

							if (copyAdjMat[out][in] > 0) {
								tempList.add(sparqlTripleMap.get(copyAdjMat[out][in]));								
							}	
						}
					}
					
					int c = count - 1;
					tempSubQueryList.put(parentQId.concat("-"+c), tempList);

				} else {
			//		subQueryType = "DISCONNECTED";
					
					String subQId1 = parentQId.concat("-"+count);
					count++;
					String subQId2 = parentQId.concat("-"+count);
					count++;
					
					HashSet<String> resultVar2 = new HashSet();
					
					for (String str: resultVarList) {
						
						if (connectedComponent1.contains(graphVertexMap.get(str))) {
							resultVar.add(str.replace("?", ""));
						} else if (connectedComponent2.contains(graphVertexMap.get(str))) {
							
							resultVar2.add(str.replace("?",""));
						}
					}
					
					String cp1Variable2 = "NULL", cp2Variable2 = "NULL", cp1Cond2 = "NULL", cp2Cond2 = "NULL", counterQ2 = "NULL";
					String expectedRelation2 = new String();
					expectedRelation2 = expectedRelation;
					
					if (connectedComponent1.contains(sId)) {
						
						cp1Variable = source.replace("?", "");
						cp1Variable2 = dest.replace("?", "");
						expectedRelation = expectedRelation.concat(" _ out");
						expectedRelation2 = expectedRelation2.concat(" _ in");
						resultVar.add(source.replace("?",""));
						resultVar2.add(dest.replace("?",""));
						counterQ = "Q : "+subQId2;
						counterQ2 = "Q : "+subQId1;
						
					} else {
						
						cp1Variable2 = source.replace("?", "");
						cp1Variable = dest.replace("?", "");
						expectedRelation2 = expectedRelation2.concat(" _ out");
						expectedRelation = expectedRelation.concat(" _ in");
						resultVar2.add(source.replace("?",""));
						resultVar.add(dest.replace("?",""));
						counterQ2 = "Q : "+subQId1;
						counterQ = "Q : "+subQId2;
					}

					String result1 = "";
					
					for (String str: resultVar) {
						result1 = result1.concat(str+", ");
					}
						
					result1 = result1.substring(0, result1.lastIndexOf(","));
					String result2 = "";
					
					for (String str: resultVar2) {
						result2 = result2.concat(str+", ");
					}
						
					result2 = result2.substring(0, result2.lastIndexOf(","));
					
					 line = parentQId+"\t"+subQId1+"\t"+result1+"\t"+cp1Variable+"\t"+cp2Variable+"\t"+expectedRelation+"\t"+
							cp1Cond+"\t"+cp2Cond+"\t"+counterQ;
					
					SubQuery squery = new SubQuery(line, dataset);
					sMap.put(subQId1,squery);
					GlobalDSReader.updateSubQueryMap(subQId1, squery);

					line = parentQId+"\t"+subQId2+"\t"+result2+"\t"+cp1Variable2+"\t"+cp2Variable2+"\t"+expectedRelation2+"\t"+
						cp1Cond2+"\t"+cp2Cond2+"\t"+counterQ2;
						
					SubQuery squery2 = new SubQuery(line, dataset);
					sMap.put(subQId2,squery2);
					GlobalDSReader.updateSubQueryMap(subQId2, squery2);

					HashSet<Integer> conComp = new HashSet();
					conComp.addAll(connectedComponent1);
					LinkedList<String> tempList = new LinkedList();
					tempList.add(result1);
					
					while (!conComp.isEmpty()) {
				
						ArrayList<Integer> l = new ArrayList(conComp);
						int out = l.get(0);		
						conComp.remove(out);
						
						for (Integer in : conComp) {

							if (copyAdjMat[out][in] > 0) {
								tempList.add(sparqlTripleMap.get(copyAdjMat[out][in]));										
							}
						}
					}
					
					int c = count-2;
					tempSubQueryList.put(parentQId.concat("-"+c), tempList);
					conComp = new HashSet();
					conComp.addAll(connectedComponent2);
					tempList = new LinkedList();
					tempList.add(result2);
					
					while (!conComp.isEmpty()) {
						
						ArrayList<Integer> l = new ArrayList(conComp);
						int out = l.get(0);
						conComp.remove(out);
						
						for (Integer in : conComp) {
							if (copyAdjMat[out][in] > 0) {
								tempList.add(sparqlTripleMap.get(copyAdjMat[out][in]));								
							}
						}
					}
					
					c = count - 1;
					tempSubQueryList.put(parentQId.concat("-"+c), tempList);
				}
			} 

		}
		
		for (Entry<String, LinkedList<String>> entry: tempSubQueryList.entrySet()) {
			
			String stmt = "SELECT ";	
			LinkedList<String> list = new LinkedList();
			list.addAll(entry.getValue());
			
			String resultVar = list.getFirst();
			list.removeFirst();
			
			String[] resComp = resultVar.split(", ");
			
			for (String s: resComp) {
				stmt = stmt.concat("?"+s+" ");
			}
			
			stmt = stmt.concat("WHERE{ ");
			String whereStmt = "";
			
			for (String str: list) {
			
				whereStmt = whereStmt.concat(str+". ");
			}
			
			stmt = stmt.concat(whereStmt);
			stmt = stmt.concat("}");
			
			subQueryList.put(entry.getKey(), stmt);
		}
				
		return subQueryList;
	}
	
	
	private static Collection<? extends HashSet<Integer>> findConnectedComp(
			int[][] copyAdjMat, int sId, int dId, int vertexCount, HashSet<Integer> constVertex) {

		HashSet<Integer> firstComp = new HashSet();
		HashSet<Integer> traversedVertex = new HashSet();
		HashSet<HashSet<Integer>> connectedComp = new HashSet();
		traversedVertex.add(sId);
		HashSet<Integer> toExplore = new HashSet();
		toExplore.add(sId);
		firstComp.add(sId);
		
		while (!toExplore.isEmpty()) {
			
			ArrayList<Integer> list = new ArrayList(toExplore);
			Integer outV = list.get(0);
			traversedVertex.add(outV);

			for (int i = 0; i < vertexCount; i++) {
				
				if (copyAdjMat[outV][i] > 0) {
					toExplore.add(i);
					firstComp.add(i);
				}
			}
			
			toExplore.removeAll(traversedVertex);
		}
		
		if (firstComp.size() > 1)
			connectedComp.add(firstComp);
		
		HashSet<Integer> secondComp = new HashSet();

		if (!firstComp.contains(dId)) {
			
			traversedVertex.clear();
			HashSet<Integer> toExplore1 = new HashSet();
			toExplore1.add(dId);
			secondComp.add(dId);
			
			while (!toExplore1.isEmpty()) {
				
				ArrayList<Integer> list = new ArrayList(toExplore1);
				Integer outV = list.get(0);
				traversedVertex.add(outV);
	
				for (int i = 0; i < vertexCount; i++) {
					
					if (copyAdjMat[outV][i] > 0) {
						toExplore1.add(i);		
						secondComp.add(i);
					}
				}
				
				toExplore1.removeAll(traversedVertex);
			}

		
			if(secondComp.size() > 1)
				connectedComp.add(secondComp);
		}
		
		return connectedComp;
	}


	public static int[][] cloneArray(int[][] src) {
	    int length = src.length;
	    int[][] target = new int[length][src[0].length];
	    for (int i = 0; i < length; i++) {
	        System.arraycopy(src[i], 0, target[i], 0, src[i].length);
	    }
	    return target;
	}
	
	
	private static HashMap<Integer, String> generateSPARQLTuples(Query query) {
		
		HashMap<Integer, String> map = new HashMap();
		int i = 1;
		ArrayList<String> tripleList = new ArrayList();
		
		ElementWalker.walk(query.getQueryPattern(),

			    new ElementVisitorBase() {

			        public void visit(ElementPathBlock el) {

			            Iterator<TriplePath> triples = el.patternElts();
			            
			            while (triples.hasNext()) {

							TriplePath triple = new TriplePath(triples.next().asTriple());

			            	String subject = triple.getSubject().toString();
			            	String object = triple.getObject().toString();
			            	String pred = triple.getPredicate().toString();
			            	
        	
			            	if (triple.getSubject().isURI())
			            		subject = "<"+subject+">";
			            	
			            	if (triple.getObject().isURI())
			            		object = "<"+object+">";
			            	
			            	pred = predicateCorrectMap.get("<"+pred.toLowerCase()+">");
			  
			            	String triple_stmt = subject+" "+pred+" "+object;
			            	tripleList.add(triple_stmt);
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

private static HashMap<Integer, String> generateTuples(Query query) {
	
		HashMap<Integer, String> map =new HashMap();
		int i = 1;
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

			            	
			            	if (predicateMapping.containsKey("<"+pred.toLowerCase()+">")) {
			            		
			            		String triple_stmt = subject+" "+predicateMapping.get("<"+pred.toLowerCase()+">")+" "+object;
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

		
}
