package main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import preprocessing.queryRegistry.annotator.SubQuery;
import virtuoso.jena.driver.VirtGraph;
import virtuoso.jena.driver.VirtuosoQueryExecution;
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory;

public class NeoQueryExecutor {

	static GraphDatabaseFactory graphDbFactory ;
	static GraphDatabaseService db ;
	static HashMap<String, String> predicateMapping;
	static HashMap<String, SubQuery> subqueryMap;
	static HashMap<String, String> vertexIdLookup;
	static HashMap<String,HashSet<String>> cypherInvertedIndex=  new HashMap();
	static String cypherQId = new String();


	public NeoQueryExecutor(boolean isFirstObject) throws IOException {
		
		subqueryMap = new HashMap();
		subqueryMap.putAll(GlobalDSReader.getSubQueryMap());

		if (isFirstObject ) {

			db = GlobalDSReader.getDBInstance();
			BufferedReader br = new BufferedReader(new FileReader(new File("./data/sqlTableName.txt")));			
			String l = "";
			predicateMapping = new HashMap();
			
			while ((l = br.readLine()) != null) {

				String lComp[] = l.split("\t");
				predicateMapping.put(lComp[0].trim().toLowerCase(), lComp[1].trim());
			}
			
			br.close();
			
			br = new BufferedReader(new FileReader(new File("/data/home/garima/GraphQueryMaintenance/meta/dbpedia/kg/raw/invovledVertexIdMap.txt")));
			vertexIdLookup  = new HashMap();

			while ((l = br.readLine()) != null) {
				
				String lComp[] = l.split("\t");
				vertexIdLookup.put(lComp[0].trim(), lComp[1].trim());
			}
		}
	}
	
	
   public static HashMap<String, String> execute(String qId, String qStmt, String qType) throws SQLException, UnsupportedEncodingException {
				
		ArrayList<String> tripleList = new ArrayList();
		HashMap<String, String> resultSet = new HashMap();
		ArrayList<String> resultVar = new ArrayList();
		String projectedVar = getProjectionVariables(qStmt);
		String newQStmt = updateQStmt(qStmt, projectedVar);
		tripleList.addAll(getTriples(qStmt));
		
//		String cypherStmt =  getCypherStatement(tripleList, projectedVar);
		String cypherStmt =  getCypherStatementWithID(tripleList, projectedVar);

		resultVar.addAll(getResultVariableNames(tripleList, projectedVar));
		
		
	 	try (Transaction tx1 = db.beginTx()) {

		
		Result rs = db.execute(cypherStmt);
		String item = "";
		
		while (rs.hasNext()) {
		
			String resultValue = "";
			String vIdList = "";	
			Map<String,Object> row = rs.next();

			if (qType.equals("PQ")) {

				for (String resVar:resultVar) {
					resultValue = resultValue.concat("?"+resVar+" = "+row.get(resVar+".uri")+", ");
				}	
					
				resultValue = resultValue.substring(0, resultValue.lastIndexOf(","));
						
			}else { // for SQ

				String cp1 = subqueryMap.get(qId).getCP1Variable();
				String cp2 = subqueryMap.get(qId).getCP2Variable();
				String resultVariable = subqueryMap.get(qId).getResultVariable();
				String aComp[] = resultVariable.split(", ");
					
				for (String a: aComp) {
							
					resultValue = resultValue.concat(a.trim()+" = "+row.get(a+".uri")+", ");
				}
					
				resultValue = resultValue.substring(0, resultValue.lastIndexOf(","));
											
				if(!cp1.equals("NULL")) {
					vIdList = vIdList.concat(cp1+"=v["+row.get(cp1+"_id")+"], ");
				}

				if(!cp2.equals("NULL"))
					vIdList = vIdList.concat(cp2+"=v["+row.get(cp2+"_id")+"], ");
					
					
				if (vIdList.length() == 0)
					vIdList = "NULL";
				else
					vIdList = vIdList.substring(0, vIdList.lastIndexOf(","));
					
				resultValue = resultValue.concat("\t"+vIdList);
			}
				
			
			String poly = (String) row.get("poly");
			
			if (resultSet.containsKey(resultValue))
				resultSet.get(resultValue).concat("+"+poly);
			else
				resultSet.put(resultValue, poly);		
		}
		
 		tx1.success();
	 	rs.close();
 	}
		
	return resultSet;		
}


private static HashSet<String> getResultVariableNames(ArrayList<String> tripleList, String projection) {

	HashSet<String> nodesVarList = new HashSet();
	
	projection = projection.trim();
	
	if (projection.equals("*")) {
		
		for (String triple : tripleList) {
			
			String tripleComp[] = triple.split(" ");
			String sub = tripleComp[0];
			String obj = tripleComp[2];
			String pred = tripleComp[1];

			if (sub.startsWith("?")) {
				sub = sub.substring(1);
				nodesVarList.add(sub);
			} 
			
			if(obj.startsWith("?")) {
				obj = obj.substring(1);
				nodesVarList.add(obj);

			}
		}
	} else {
		
		projection = projection.trim();
		String projComp[] = projection.split(", ");
		
		for (String s: projComp) {
		
			if(!s.equals("DISTINCT")) {
				s = s.substring(1);	
				nodesVarList.add(s);
			}
		}
	}

	return nodesVarList;
}


private static ArrayList<String> getTriples(String qStmt) {
	
	ArrayList<String> tripleList = new ArrayList();
	
	String qStmtComp[] = qStmt.split("WHERE");
	String t1 = qStmtComp[1];

	String temp = t1.substring(t1.indexOf("{")+1, t1.lastIndexOf("}") );
	temp = temp.trim();
	String triples[] = temp.split(" ");
	String triple = "";
	int count = 0;
			
  	for (String str : triples) {

		if (str.length() > 0) {
			triple = triple.concat(str+" ");
			count++;
		}
				 
		if (count%3 == 0) {
			triple = triple.substring(0, triple.length()-2);
			tripleList.add(triple);
			triple = "";
		}
	}

  	return tripleList;
}


private static String getCypherStatement(ArrayList<String> tripleList, String projection) {
String neoStmt = "";	
	
	String matchStmt = "MATCH ";
	int i = 1;
	HashSet<String> nodesVarList = new HashSet();
	
	String previousObj = "";
	
	for (String triple : tripleList) {
		
		String tripleComp[] = triple.split(" ");
		String sub = tripleComp[0];
		String obj = tripleComp[2];
		String pred = tripleComp[1];
		pred = predicateMapping.get(pred.toLowerCase());
		pred = "`"+pred+"`";
		boolean isSubjectSame = false;

		
		if (sub.startsWith("?")) {
			sub = sub.substring(1);
			nodesVarList.add(sub);
		} else {
			sub = sub.substring(1,sub.lastIndexOf(">"));
			sub = "sub"+i+":entity {uri: '"+sub+"'}";
		}

		if (previousObj.equals(sub)) {		
			sub = previousObj; 
			isSubjectSame = true;
		}
		
				
		if(obj.startsWith("?")) {
			obj = obj.substring(1);
			nodesVarList.add(obj);

		} else {
			
			if (obj.startsWith("\"")) {
				obj = obj.substring(2,obj.length()-2);
		
			} else {
				obj = obj.substring(1,obj.lastIndexOf(">"));
			}
			
			obj = "obj"+i+":entity {uri: '"+obj+"'}";
		}
		
		
		if (isSubjectSame) {
			matchStmt = matchStmt.substring(0,matchStmt.lastIndexOf(","));
			matchStmt = matchStmt.concat("-[r"+i+":"+pred+"]-> ("+obj+"), ");
		
		}	else {		
			matchStmt = matchStmt.concat("("+sub+") -[r"+i+":"+pred+"]-> ("+obj+"), ");
		}

		i++;
		previousObj= obj;
	}
	
	matchStmt = matchStmt.substring(0, matchStmt.lastIndexOf(", "));	
	String returnStmt = " RETURN ";
	String nodeStmt = "";
	String polyStmt = "";
	
	if (projection.equals("*")) {
	
		for (String x: nodesVarList) {
			nodeStmt = nodeStmt.concat(x+".uri, id("+x+") as "+x+"_id, ");
		}
	} else {
		
		projection = projection.trim();
		String projComp[] = projection.split(", ");
		
		for (String s: projComp) {
			
			if(!s.equals("DISTINCT")) {
				s = s.substring(1);
				nodeStmt = nodeStmt.concat(s+".uri, id("+s+") as "+s+"_id, ");
			
			} else{	
				nodeStmt = nodeStmt.concat("DISTINCT ");						
			}
		}
	}
	
	for (int k = 1; k < i; k++) {
			polyStmt = polyStmt.concat("'.e'+r"+k+".edge_id+");
	}
	
	polyStmt = polyStmt.substring(2, polyStmt.lastIndexOf("+"));
	polyStmt = "'".concat(polyStmt);
	polyStmt = polyStmt+" as poly";
	neoStmt = matchStmt+returnStmt+nodeStmt+polyStmt;
	
	return neoStmt;

}


private static String getCypherStatementWithID(ArrayList<String> tripleList, String projection) {

	String neoStmt = "";	
	String matchStmt = "MATCH ";
	int i = 1;
	HashSet<String> nodesVarList = new HashSet();
	String previousObj = "";
	String whereCond = "";
	HashMap<String, String> literalsMap = new HashMap();
	
	for (String triple : tripleList) {
		
		String tripleComp[] = triple.split(" ");
		String sub = tripleComp[0];
		String obj = tripleComp[2];
		String pred = tripleComp[1];
		String subVar = "";
		String objVar = "";
		pred = predicateMapping.get(pred.toLowerCase());
		
		if(pred.equals("dbpopulation_metro"))
			System.out.println(triple);
		
		if(cypherInvertedIndex.containsKey(pred)) {
			cypherInvertedIndex.get(pred).add(cypherQId);
		
		} else {
			HashSet<String> s = new HashSet();
			s.add(cypherQId);
			cypherInvertedIndex.put(pred, s);
			System.out.println(pred);
		}
		
		pred = "`"+pred+"`";	
		boolean isSubjectSame = false;

		if (sub.startsWith("?")) {
			subVar = sub.substring(1);
			nodesVarList.add(subVar);

		} else {
			sub = sub.substring(1,sub.lastIndexOf(">"));
			String id=  vertexIdLookup.get(subVar);

			if (literalsMap.containsKey(sub)) 
				subVar = literalsMap.get(sub);
			else {
				subVar = "sub"+i;
				literalsMap.put(sub, subVar);
			}
			
			whereCond = whereCond.concat(subVar+".vid = \""+id +"\" AND ");			
		}
		

		if (previousObj.equals(sub)) {
			
			sub = previousObj; 
			isSubjectSame = true;
		}
		
		if(obj.startsWith("?")) {
			objVar = obj.substring(1);
			nodesVarList.add(objVar);

		} else {
			
			if (obj.startsWith("\"")) {
				objVar = obj.substring(2,obj.length()-2);
			
			} else {
				objVar = obj.substring(1,obj.lastIndexOf(">"));
			}
			
			String id=  vertexIdLookup.get(objVar);
			
			if (literalsMap.containsKey(obj)) {
				objVar = literalsMap.get(obj);
			
			} else {
				objVar = "obj"+i;
				literalsMap.put(obj, objVar);
			}
			
			whereCond = whereCond.concat(objVar+".vid = \""+id +"\" AND ");
		}
				
		if (isSubjectSame) {
			matchStmt = matchStmt.substring(0,matchStmt.lastIndexOf(","));
			matchStmt = matchStmt.concat("-[r"+i+":"+pred+"]-> ("+objVar+"), ");
		}	
		else {		
			matchStmt = matchStmt.concat("("+subVar+") -[r"+i+":"+pred+"]-> ("+objVar+"), ");
		}

		i++;
		previousObj= obj;
	}
	
	matchStmt = matchStmt.substring(0, matchStmt.lastIndexOf(", "));	
	String returnStmt = " RETURN ";
	String nodeStmt = "";
	String polyStmt = "";
	
	if (projection.equals("*")) {
	
		for (String x: nodesVarList) {
			nodeStmt = nodeStmt.concat(x+".uri, id("+x+") as "+x+"_id, ");
		}
	} else {
		
		projection = projection.trim();
		String projComp[] = projection.split(", ");
		
		for (String s: projComp) {
			
			if(!s.equals("DISTINCT")) {

				s = s.substring(1);	
				nodeStmt = nodeStmt.concat(s+".uri, id("+s+") as "+s+"_id, ");
			} else{
				
				nodeStmt = nodeStmt.concat("DISTINCT ");						
			}
		}
	}
	
	for (int k = 1; k < i; k++) {
			polyStmt = polyStmt.concat("'e'+r"+k+".edge_id+");
	}
	
	if(whereCond.length() > 0) {
		whereCond = whereCond.substring(0, whereCond.lastIndexOf("AND"));
		whereCond = " WHERE "+whereCond;
	}
	
	polyStmt = polyStmt.substring(0, polyStmt.lastIndexOf("+"));	
	polyStmt = polyStmt+" as poly";
	neoStmt = matchStmt+whereCond+returnStmt+nodeStmt+polyStmt;
	
	return neoStmt;

}

private static String updateQStmt(String qStmt, String projectedVar) {
	
	String qComp[] = qStmt.split("WHERE");
	String qComp1[] = qStmt.split("SELECT");
	String newQStmt = qComp1[0].concat(" SELECT * WHERE").concat(qComp[1]);  
	
	return newQStmt;
}

private static String getProjectionVariables(
		String qStmt) {
		
		String projectedVariables = new String();
		
		String[] strComp = qStmt.split("SELECT");
		
		if(strComp.length < 2) {
			System.out.println("Query format incorrect: Cannot find projection!!");
			System.exit(0);
		
		} else {
			
			String[] interList = strComp[1].split("WHERE");
			String[] projList = interList[0].split(" ");
			
			for(String var: projList) {
				
				if (var.equals("*")) {
					projectedVariables = "*,";
					
				} else if (var.length()>0 && var.startsWith("?")) {
					projectedVariables = projectedVariables.concat(var.trim()+", ");
				}
			}

			projectedVariables = projectedVariables.substring(0, projectedVariables.lastIndexOf(","));
		}
		
		return projectedVariables;
	}

}
