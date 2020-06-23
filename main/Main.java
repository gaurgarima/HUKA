package main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

//import org.apache.jena.ext.com.google.common.collect.ArrayListMultimap;

import maintenance.deletion.DeletionHandler;
import maintenance.insertion.InsertionHandler;
import preprocessing.queryRegistry.queryExecutor.PolynomialBuilder;

public class Main {

	static String dataset;
	
	public static void main(String[] args) throws IOException, NumberFormatException, ClassNotFoundException, SQLException, CloneNotSupportedException {

		/*
		 * Read create configuratino Reader with dataset
		 * Read toChange file
		 * Create insertion handler and deletion handler for each request
		 * 
		 */		
		ConfigurationReader.readConfiguration();
		dataset = ConfigurationReader.get("DATASET");
		long dsSt, dsEt;
		dsSt = System.nanoTime();
		GlobalDSReader.instantiate();
		dsEt = System.nanoTime();
		System.out.println("Read DS");
		Statement statement = GlobalDSReader.getSQLStatement();
		Connection connect = GlobalDSReader.connect;
		
	//	registerParentQuery();

		String subId, subject, objId, object, relation, edgeId, operation;
		String line = "";
		String lineComp[] = null;		
		String setting = "15-80-1";
		
		BufferedReader br = new BufferedReader(new FileReader(new File("./experiment/"+dataset+"/workload/"+setting+".txt")));
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File("./experiment/"+dataset+"/runtime/"+setting+".txt")));
		BufferedWriter bw1 = new BufferedWriter(new FileWriter(new File("./experiment/"+dataset+"/runtime/responseTime_"+setting+".txt")));

		BufferedWriter bw2 = new BufferedWriter(new FileWriter(new File("./experiment/correct/"+dataset+"/toDelete_"+setting+".txt")));
		BufferedWriter bw3 = new BufferedWriter(new FileWriter(new File("./experiment/correct/"+dataset+"/toAdd_"+setting+".txt")));
		
		BufferedWriter bw4 = new BufferedWriter(new FileWriter(new File("./experiment/correct/"+dataset+"/split/deletion_"+setting+".txt")));
		BufferedWriter bw5 = new BufferedWriter(new FileWriter(new File("./experiment/correct/"+dataset+"/split/insertion_"+setting+".txt")));


		int count = 1;
		
		while ((line = br.readLine()) != null) {
						
			lineComp = line.split("\t");
			
			subId = lineComp[1].trim();
			subject = lineComp[0].trim();
			objId = lineComp[3].trim();
			object = lineComp[2].trim();
			relation = lineComp[4].trim();
			edgeId = lineComp[5].trim();
			operation = lineComp[6].trim();
		
			String splitTime = "";
			Long responseTime = (long) 0;
			Integer affectedQueryCount = -1;
			Long st = (long) -1,et = (long) -1;
			boolean specialInsertion = false;

			if (operation.equals("D")) {
		
				PreparedStatement preparedStatement = connect.prepareStatement("Delete from  `"+relation+"` where `out` = ? and `in` = ? and poly = ?");	 
	    			preparedStatement.setString(1, subject);		
	           		preparedStatement.setString(2, object);		
	           		preparedStatement.setString(3, "e"+edgeId);		
	           		preparedStatement.executeUpdate();
	        
	       			bw3.write(relation+"\t"+subject+"\t"+object+"\t"+edgeId+"\n");
				bw3.flush();

				DeletionHandler handler = new DeletionHandler(dataset);
				st = System.nanoTime();
				splitTime = handler.delete(edgeId);
				String time = handler.update(edgeId);
				et = System.nanoTime();
				splitTime = splitTime+"\t"+time;
			
				bw4.write(relation+"\t"+edgeId+"\t"+splitTime+"\n");
				bw4.flush();
			
			} else if (operation.equals("I")) {
				/*
				 * Update insertion handler to handle parent queries of size two
				 */
				
			 	PreparedStatement preparedStatement = connect.prepareStatement("insert into  `"+relation+"` values (?, ?, ?)");
	   
			 	if (subject.length() > 500) {
			 		subject = subject.substring(0, 499);
			 	}
			 	
			 	if (object.length() > 500) {
			 		object = object.substring(0, 499);
			 	}
			 	 		
	    			preparedStatement.setString(1, subject);		
	  		        preparedStatement.setString(2, object);		
	           		preparedStatement.setString(3, "e"+edgeId);		

	   		        preparedStatement.executeUpdate();
	            
	    		        bw2.write(relation+"\t"+subject+"\t"+object+"\t"+edgeId+"\n");
				bw2.flush();
				
				specialInsertion = false;

				InsertionHandler handler = new InsertionHandler(dataset);
				
				st = System.nanoTime();

				if (GlobalDSReader.isPredicateSpecial(relation)) {
					
					splitTime = handler.insertionWithSpecialQuery(subject, object, relation, edgeId, Long.parseLong(subId), Long.parseLong(objId));
					specialInsertion = true;
				} else { 
					splitTime = handler.insertion(subject, object, relation, edgeId, Long.parseLong(subId), Long.parseLong(objId));
				}
					
				et = System.nanoTime();	
			}
		
			if (specialInsertion) {
				
				bw.write(count+"\t"+line+"\t"+Long.toString(et-st)+"\tS\n");
				bw.flush();

				System.out.println(count+"\t"+line+"\t"+Long.toString(et-st)+"\tS");
				specialInsertion = false;
				bw5.write(relation+"\t"+edgeId+"\t"+splitTime+"\tS\n");
				
			} else {
				
				bw.write(count+"\t"+line+"\t"+Long.toString(et-st)+"\n");
				bw.flush();
			
				System.out.println(count+"\t"+line+"\t"+Long.toString(et-st));

				if(operation.equals("I"))
					bw5.write(relation+"\t"+edgeId+"\t"+splitTime+"\n");
			}
		
			bw5.flush();
			
			count++;
		}

		br.close();
		bw.close();
		bw2.close();
		bw3.close();
		bw4.close();
		bw5.close();
 		
 		long closeSt, closeEt;
		
		closeSt = System.nanoTime();
		GlobalDSReader.close();
		closeEt = System.nanoTime();
		
		System.out.println("\n globalDS Instantiation time: "+Long.toString((dsEt-dsSt)));
		System.out.println("\n globalDS closing time: "+Long.toString((closeEt-closeSt)));		
	}

	
	private static void registerParentQuery() throws IOException, ClassNotFoundException, CloneNotSupportedException, SQLException {

		BufferedReader br = new BufferedReader(new FileReader(new File ("./workload/finalQListtemp.txt"))); // TODO: correct the file name
	
		String prefix = "BASE <http://yago-knowledge.org/resource/> PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> ";
		
		prefix = "";
		GlobalDSReader.instantiate();

		String line = "";
		int count = 1;
		boolean isFirst = false;

		while ((line=br.readLine()) != null) {
				
			if(count == 0)
				isFirst = true;
			
			String lineComp[] = line.split("\t");
			String qStmt = lineComp[2];
			String qId = lineComp[0];
			
			long st = System.nanoTime();
			qStmt = prefix.concat(qStmt);
			
			String status = registerQuery(qStmt,qId, isFirst);
			long et = System.nanoTime();
		
			System.out.println(count+"\t"+qId+"\t"+lineComp[1]+"\t"+Long.toString((et-st))+"\t"+status+"\n");	
			count++;
			
			GlobalDSReader.saveMaps();	
			System.out.println("Maps saved with"+count);
	
			isFirst = false;
			
		}
	
		br.close();
		GlobalDSReader.close();

	}


	private static String registerQuery(
			String qStmt,
			String qId, boolean isFirst) throws ClassNotFoundException, IOException, CloneNotSupportedException, SQLException {

/*
		QueryRegistryEngine registry = new QueryRegistryEngine(qId, qStmt);		
		return registry.registerSubQuery(qStmt, qId, isFirst);
*/		
		return "";
	}	
}
