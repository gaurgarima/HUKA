import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.syntax.ElementPathBlock;
import org.apache.jena.sparql.syntax.ElementVisitorBase;
import org.apache.jena.sparql.syntax.ElementWalker;




/**
*	This class examine each query syntax by passing it to Jena query parser, and assign ids and find query size as well.
*	It also collects the predicate invovled in the query set to construct SQL tables
*
**/

public class QueryParser {
	
	// For testing
	static HashSet<String> predicateSet;
	
	public static void main(String[] args) throws FileNotFoundException, IOException {
		examiner(args[0]);
	
	}


	static void examiner (String dataset) throws FileNotFoundException, IOException{

	/*	ConfigurationReader config = new ConfigurationReader();
		config.readConfiguration();
	*/
		//String dataset = config.get("DATASET") ;
		String rawQueryFilePath = "./rawQueryList.txt";

		String finalQueryFilePath = "./queryList.txt";

		String sqlTableNameMapFilePath = "./queryPredList.txt" ;

		predicateSet = new HashSet();
		
		BufferedReader br = new BufferedReader(new FileReader(new File(rawQueryFilePath)));
		BufferedWriter bw = new BufferedWriter (new FileWriter(new File(finalQueryFilePath)));
		BufferedWriter bw1 = new BufferedWriter (new FileWriter(new File(sqlTableNameMapFilePath)));

		String line = "";
		String lineComp[] = null;
		int queryId = 1;
		int querySize = 0;

		while ((line = br.readLine()) != null) {

			try {
				Query query = QueryFactory.create(line);

				querySize = jenaParser(query);

			} catch(Exception e) {

				continue;
			}

			bw.write(queryId+"\t"+querySize+"\t"+line+"\n");
			queryId++;
		}

		bw.close();
		br.close();


		for (String pred: predicateSet) {

			bw1.write(pred+"\n");
		}

		bw1.close();
	}


	private static int jenaParser(Query query) {
	
		HashMap<Integer, String> map =new HashMap();
		int i = 1;
		ArrayList<String> tripleList= new ArrayList();
		 int count = 0;
		
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
			            	String pred = triple.getPredicate().toString();
					String subject = triple.getSubject().toString();
			            	String object = triple.getObject().toString();
			            

			            	if(!pred.startsWith("?")) {
			            		predicateSet.add("<"+pred+">");
			            	}

					String triple_stmt = subject+" "+pred+" "+object;
			            	tripleList.add(triple_stmt);

			            	//++count;

			            }
			        }
			    }
			);

		count = tripleList.size();
		return count;
	}


/*
	private static HashMap<Integer, String> generateSPARQLTuples(Query query) {
	
		HashMap<Integer, String> map =new HashMap();
		int i = 1;
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
			            	
			            	
			            //	String subject = triple.getSubject().toString();
			            //	String object = triple.getObject().toString();
			            	String pred = triple.getPredicate().toString();


			            	if(!pred.startsWith("?")) {
			            		predicateSet.add("<"+pred+">");
			            	}
			            	
			           // 	System.out.println(subject);
			          //  	System.out.println(pred);
			          //  	System.out.println(object);
		         	
			 
			 /*
			            	
			            	if (triple.getSubject().isURI())
			            		subject = "<"+subject+">";
			            	
			            	if (triple.getObject().isURI())
			            		object = "<"+object+">";
			            	
			            	pred = predicateCorrectMap.get("<"+pred.toLowerCase()+">");
			   */

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
/*			            }
			        }
			    }
			);
		
		for (String str: tripleList) {
		//	System.out.println(i+"\t"+str);
			map.put(i, str);
			i++;
		}

		return map;
	}
*/

}
