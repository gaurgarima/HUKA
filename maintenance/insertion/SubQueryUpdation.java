package maintenance.insertion;

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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.syntax.ElementPathBlock;
import org.apache.jena.sparql.syntax.ElementVisitorBase;
import org.apache.jena.sparql.syntax.ElementWalker;

import main.ConfigurationReader;
import main.GlobalDSReader;
import preprocessing.queryRegistry.executionPlan.global.BaseRelation;
import preprocessing.queryRegistry.executionPlan.global.InsertionPropogationWithSQL;
import preprocessing.queryRegistry.executionPlan.global.InternalRelation;
import preprocessing.queryRegistry.executionPlan.local.AndOrGraph;


/**
 * Default class of each package; 
 * @author garima
 *
 */

public class SubQueryUpdation {

	static ArrayList<AndOrGraph> andOrGraphList;
	static HashMap<String, BaseRelation> baseRelationMap;
	static HashMap<String, InternalRelation> internalRelationMap;
	static String dataset; 
	static InsertionPropogationWithSQL propagator;
	
	public SubQueryUpdation (String database) {
		
		long et, st;
		long propagatorEt = 0, propagatorSt = 0;
		this.dataset = database;
		
		try {
			
			baseRelationMap = new HashMap();
			internalRelationMap = new HashMap();
			baseRelationMap.putAll(GlobalDSReader.getBaseRelationMap());
			internalRelationMap.putAll(GlobalDSReader.getInternalRelationMap());
			propagator = new InsertionPropogationWithSQL(baseRelationMap, internalRelationMap,dataset.toUpperCase());			
		} catch (
				ClassNotFoundException
				| IOException | SQLException e) {
			e.printStackTrace();
		}
	}
		
	
	public InsertionPropogationWithSQL getPropagator() {
		return propagator;
	}
	
	public long insert (String subject, String object, String relation, String edgeId) throws IOException, SQLException, ClassNotFoundException {
		
		long totalTimeTaken = 0;
		long st,et, deltaComputationEt, annotationUpdateEt, edgeMapUpdateEt;
		
		st = System.nanoTime();
		propagator.updateSubQueryRevised(subject, relation, object, "0", 0, edgeId);	 
		deltaComputationEt = System.nanoTime();
			 
		return deltaComputationEt-st ;
	}

	public static String updateDS() throws SQLException, ClassNotFoundException, IOException {
		
		Long deltaComputationEt = System.nanoTime();
		propagator.updateCPWithNeo(propagator.getComputedResults());
		Long annotationUpdateEt = System.nanoTime();
		
		propagator.updateEdgeToCPMap(propagator.getGlobalCPMap());
		Long edgeMapUpdateEt = System.nanoTime();	

		propagator.cleanUp();
		propagator.closeConnections();
		 
		Long et = System.nanoTime();
		 
		return Long.toString(edgeMapUpdateEt-annotationUpdateEt)+"\t"+Long.toString(annotationUpdateEt - deltaComputationEt)+"\t"+Long.toString(et-edgeMapUpdateEt);
	}
	

	private static void loadPlan () throws IOException, ClassNotFoundException {

		String baseNodeMapPath = ConfigurationReader.get("BASE_NODE_MAP");
		String internalMapPath = ConfigurationReader.get("INTERNAL_NODE_MAP");
		
		FileInputStream file1 = new FileInputStream(baseNodeMapPath);
		ObjectInputStream object1 = new ObjectInputStream(file1);
    
		baseRelationMap = (HashMap<String, BaseRelation>) object1.readObject();
		object1.close();
		 
		FileInputStream file3 = new FileInputStream(internalMapPath);
		ObjectInputStream object3 = new ObjectInputStream(file3);
     
		internalRelationMap = (HashMap<String, InternalRelation>) object3.readObject();
		object3.close();
		
	}
	
	
	private static void buildGraph(ArrayList<String> queryList) {
		int startENodeId, startOpNodeId;
		startENodeId = 1;
		startOpNodeId = 1;
	
		for (String query_stmt: queryList) {
			Query query = QueryFactory.create(query_stmt);
			HashMap<Integer, String> map = new HashMap();
			map.putAll(generateTuples(query));
			
			ArrayList<String> toProject = new ArrayList();
			toProject.addAll(getProjection(query_stmt));
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
			            	
			            	if (!subject.startsWith("?")) {
			            		
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
	
	private static HashSet<String> getProjection(String query_stmt) {
		
		HashSet<String> projectedVariables = new HashSet();
		String[] strComp = query_stmt.split("SELECT");
		
		if(strComp.length < 2) {
			System.out.println("Query format incorrect: Cannot find projection!!");
			System.exit(0);
		
		} else {
			
			String[] interList = strComp[1].split("WHERE");
			String[] projList = interList[0].split(" ");
			
			for(String var: projList) {
				
				if (var.length()>0) {
					projectedVariables.add(var.trim());
				}
			}	
		}
		
		return projectedVariables;
	}
	
}
