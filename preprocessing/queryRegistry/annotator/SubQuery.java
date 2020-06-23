package preprocessing.queryRegistry.annotator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import main.ConfigurationReader;

public class SubQuery implements java.io.Serializable  {

	private String cp1VariableName;
	private String cp2VariableName; 
	private String expectedRelation; 
	private String queryId; 
	private String cp1Condition; 
	private String cp2Condition; 
	private String counterQuery; 
	private String resultVariable; 
	private String parentQueryId; 
	private String direction;
	private ArrayList<String> logicalOpList;
	private ArrayList<String> relationalOpList;
	 Connection connect = null;
	 Statement statement = null;
	private String db;

	public SubQuery(String line, String db) {
		
		String lineComp[] = line.split("\t");
		
		cp1VariableName = new String();
		cp2VariableName = new String();
		expectedRelation = new String();
		queryId = new String();
		cp1Condition = new String();
		cp2Condition = new String();
		counterQuery = new String();
		resultVariable = new String();
		parentQueryId = new String();
		direction = new String();
		
		logicalOpList = new ArrayList();
		relationalOpList = new ArrayList();
		
		
		parentQueryId = lineComp[0];
		queryId = lineComp[1];
		resultVariable = lineComp[2];
		cp1VariableName = lineComp[3];
		cp2VariableName = lineComp[4];
		expectedRelation = lineComp[5].split(" _ ")[0];
		
		if (expectedRelation.contains(":")) {
			expectedRelation = expectedRelation.split(":")[1];
		}

		
		direction = lineComp[5].split(" _ ")[1];
		cp1Condition = lineComp[6];
		cp2Condition = lineComp[7];
		counterQuery = lineComp[8];
		this.db = db;
		
		initialzeOp();

	}
	
	void connectToDB() {
		
        try {
        	String username = ConfigurationReader.get("SQL_USERNAME");
        	String passwrd = ConfigurationReader.get("SQL_PASSWD");
        
        	connect = DriverManager
			        .getConnection("jdbc:mysql://localhost/"+db.toUpperCase()+"?"
			                + "user="+username+"&password="+passwrd);
            statement = connect.createStatement();

        //	System.out.println("Connected to "+db);
			
            
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

            // Statements allow to issue SQL queries to the database

	}
	

	public boolean checkCPCondition(String cpVal ){
		
		cpVal = cpVal.trim();
		
		if (cp1Condition.equals("NULL"))
			return true;
		
		ArrayList<String> expressions = findExpressions(cp1Condition);
		HashMap<String, Integer> expEval = new HashMap();
		
		for (String expr: expressions) {
			
			String comp[] = expr.split(" : ");
			
			switch(comp[0]){
				
			case "eq": if(cpVal.equals(comp[1]))
								expEval.put(expr, 1);
								else
									expEval.put(expr, 0);
			
								break;
			
			case "neq": if(cpVal != comp[1])
							expEval.put(expr, 1);
						else
							expEval.put(expr, 0);

						break;
									
			case "leq": if(Double.parseDouble(cpVal) <= Double.parseDouble(comp[1]))
							expEval.put(expr, 1);
						else
							expEval.put(expr, 0);

						break;

			case "geq": if(Double.parseDouble(cpVal) >= Double.parseDouble( comp[1]))
							expEval.put(expr, 1);
						else
							expEval.put(expr, 0);

						break;
								
			case "lt": if(Double.parseDouble(cpVal) < Double.parseDouble(comp[1]))
							expEval.put(expr, 1);
						else
							expEval.put(expr, 0);

						break;
								
			case "gt": if(Double.parseDouble(cpVal) > Double.parseDouble( comp[1]))
							expEval.put(expr, 1);
						else
							expEval.put(expr, 0);
						break;
				
			default: System.out.println("*****"+comp[0]+"  CP..Invalid relational operator !!");
						break;
			
			} // End of switch
		
		}// for expression
		
		
		boolean result = false;
		int flag1 = 0;
		String op_str = getLogicalOp(cp1Condition);
		// initialize it correctly
		for(Entry<String, Integer> entry: expEval.entrySet()){
			
			Boolean val;
			
			if(entry.getValue() == 1)
				val = true;
			else
				val = false;
			
			if(flag1 == 0){			
				result = val;
				flag1 = 1;
			}
			else{
				
				switch(op_str.charAt(0)){
					
					case '|': result = result || val;
							  op_str = op_str.substring(1);
							  break;
										
					case '&': result = result && val;
							  op_str = op_str.substring(1);
							  break;
					
					default: System.out.println(" Invalid logical operator");
							 break;
				}
			
			}
			
		}
		
		return result;
		
	}
	
	// check_CP2_cond for the case where the disconnected sub graph is  isolated
	public boolean checkCP2Condition(String cpVal ){
		
		cpVal = cpVal.trim();

		if(cp2Condition.equals("NULL"))
			return true;
		
		ArrayList<String> expressions = findExpressions(cp2Condition);
		HashMap<String, Integer> exp_eval = new HashMap();
		
		for(String expr: expressions){
		
		//	System.out.println(cp_val+"\t"+expr);
			String comp[] = expr.split(" : ");
			comp[0] = comp[0].trim();
			comp[1] = comp[1].trim();
			
			switch(comp[0]){
				
			case "eq": if(cpVal.equals(comp[1]))
							exp_eval.put(expr, 1);
					   else
							exp_eval.put(expr, 0);
			
						break;
			
			case "neq": if(cpVal != comp[1])
							exp_eval.put(expr, 1);
						else
							exp_eval.put(expr, 0);

						break;
									
			case "leq": if(Double.parseDouble(cpVal) <= Double.parseDouble(comp[1]))
							exp_eval.put(expr, 1);
						else
							exp_eval.put(expr, 0);

						break;

			case "geq": if(Double.parseDouble(cpVal) >= Double.parseDouble( comp[1]))
							exp_eval.put(expr, 1);
						else
							exp_eval.put(expr, 0);

						break;
								
			case "lt": if(Double.parseDouble(cpVal) < Double.parseDouble(comp[1]))
							exp_eval.put(expr, 1);
						else
							exp_eval.put(expr, 0);
						break;
								
			case "gt": if(Double.parseDouble(cpVal) > Double.parseDouble( comp[1]))
							exp_eval.put(expr, 1);
						else
							exp_eval.put(expr, 0);
						break;
				
			default: System.out.println("Invalid relational operator !!");
						break;
			
			} // End of switch
		
		}// for expression
		
		
		boolean result = false;
		int flag1 = 0;
		String op_str = getLogicalOp(cp2Condition);			// initialize it correctly
		
		//System.out.println("op_str: "+op_str);
		
		for(Entry<String, Integer> entry : exp_eval.entrySet()){
			
			Boolean val;
			
			if( entry.getValue() == 1)
				val = true;
			else
				val = false;
			
			//System.out.println(entry.getKey().toString()+"\t"+ entry.getValue().toString());
			
			if(flag1 == 0){
			
				result = val;
				flag1 = 1;
			}
			else{
				
				switch(op_str.charAt(0)){
					
					case '|': result = result || val ;
							  op_str = op_str.substring(1);
							  break;
										
					case '&': result = result && val;
							  op_str = op_str.substring(1);
							  break;
					
					default: System.out.print(" Invalid logical operator");
							 break;
				}
				
			}
			
		}
		
		return result;
		
	}

	//check_CP2_cond for the case where disconnected subgraph is stored in a TDB
/*	boolean check_CP2_cond_old(String val, String relation, String direction){
		
		
*/	
		// this one with sql instead of jena
	public String checkCP2Condition (String val, String relation, String direction) throws NumberFormatException, SQLException{
			
			
			String result = "";
			
			String relationDirection = direction;
			
			if(relation.contains(":"))
				relation = relation.split(":")[1];
			
			connectToDB();
			String projectedAttribute = new String();
			String comparisonAttribute = new String();
			
			if (relationDirection.equals("in"))
				projectedAttribute = "out";
			else
				projectedAttribute = "in";
			
			comparisonAttribute = relationDirection;
			
			String tableName = relation;
			String comparisonValue = val;
				
				
				
			String queryStmt = "Select `"+projectedAttribute+"` as result, poly from "+tableName+" where `"+comparisonAttribute+"` = '"+comparisonValue+"';";
			
		//	System.out.println("Query is: "+queryStmt);
			
	        ResultSet rs = statement.executeQuery(queryStmt);
			
			String outV = "", inV = "", dirFlag = "";
			
			if(relationDirection.equals("in")){
				
				inV = val.trim();
				dirFlag = "in";		
			}
			else{
				outV = val.trim();
				dirFlag = "out";
										
			}			
			
			String resultVal;
			String poly ;
			//String finalPoly = new String();
			boolean flag = false;
			
			while(rs.next()){
				
			//	System.out.println("Result is not empty!!");
				
				resultVal = rs.getString("result");
				poly = rs.getString("poly");
				
			//	System.out.println("Result: "+resultVal+"\t"+poly);

				if(cp2Condition.equals("NULL"))	{
					flag = true;
					result = result.concat(poly+" . ");
					
					
				} else { 
					
				if(dirFlag.equals("in"))
					outV = resultVal.trim();
				else
					inV = resultVal.trim();		
				
			//	System.out.println("Outv: "+outV+"\t inV: "+inV);
				
				ArrayList<String> expressions = findExpressions(cp2Condition);
				HashMap<String, Integer> expEval = new HashMap();
				String cpVal;
				
			//	System.out.println("Expressions:"+expressions.toString());
				
				for(String expr : expressions){
					
					String comp[] = expr.split(" : ");
					//System.out.println()
					
					if(comp[2].equals("in"))
						cpVal = inV;
					else
						cpVal = outV;
					
				//	System.out.println("Check:"+comp[2]+"\t"+cp_val);
					
					
					switch(comp[0]){
						
					case "eq": //System.out.println("Two things to compare: "+cp_val+"\t"+comp[1]);
								if(cpVal.equals(comp[1]))
									expEval.put(expr, 1);
								else
									expEval.put(expr, 0);
					
								break;
					
					case "neq": if(!cpVal.equals(comp[1]))
									expEval.put(expr, 1);
								else
									expEval.put(expr, 0);

								break;
											
						case "leq": if(Double.parseDouble(cpVal) <= Double.parseDouble(comp[1]))
										expEval.put(expr, 1);
									else
										expEval.put(expr, 0);

									break;

						case "geq": if(Double.parseDouble(cpVal) >= Double.parseDouble( comp[1]))
										expEval.put(expr, 1);
									else
										expEval.put(expr, 0);

									break;
										
					case "lt": if(Double.parseDouble(cpVal) < Double.parseDouble(comp[1]))
									expEval.put(expr, 1);
								else
									expEval.put(expr, 0);

								break;
										
					case "gt": if(Double.parseDouble(cpVal) > Double.parseDouble( comp[1]))
									expEval.put(expr, 1);
								else
									expEval.put(expr, 0);
								break;
						
					default: System.out.print("Invalid relational operator !!");
							 System.exit(0);
							 break;
					
					} // End of switch
				
				}// for expression
				
				
				boolean resultTurn = false;
				int flag1 = 0;
				String opStr = getLogicalOp(cp2Condition);			// initialize it correctly
				
				for(Entry<String, Integer> entry : expEval.entrySet()){
					
					Boolean val1;
					
					if(entry.getValue() == 1)
						val1 = true;
					else
						val1 = false;
					
					if(flag1 == 0){
						
						resultTurn = val1;
						flag1 = 1;
					}
					else{
						
						switch(opStr.charAt(0)){
							
							case '|': resultTurn = resultTurn || val1 ;
									  opStr = opStr.substring(1);
									  break;
												
							case '&': resultTurn = resultTurn && val1 ;
									  opStr = opStr.substring(1);
									  break;
							
							default: System.out.print(" Invalid logical operator");
									 System.exit(0);
                            		 break;
						}				
					}
					
				}
				

				if(resultTurn) {
					  closeDBConnection();
					  return poly;
				}		
			}
		}	
			 closeDBConnection();
			 
			 if(flag)
				 return result.substring(0,result.lastIndexOf("."));
			 else
				 return result;
	}

		
	private void closeDBConnection() {

		  try {
			statement.close();
            connect.close();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public String getDirection() {
		  return this.direction;
	  }
	  
	public String getParentQueryId() {
			// TODO Auto-generated method stub
			return parentQueryId;
		}

	public String getExpectedRelation() {
			// TODO Auto-generated method stub
			return expectedRelation;
		}

	public String getCounterQuery() {
			// TODO Auto-generated method stub
			return counterQuery;
		}

	public String getCP2Condition() {
			// TODO Auto-generated method stub
			return cp2Condition;
		}

	public String getCP1Condition() {
			// TODO Auto-generated method stub
			return cp1Condition;
		}

	public String getResultVariable() {
			// TODO Auto-generated method stub
			return resultVariable;
		}

	public String getCP2Variable() {
			// TODO Auto-generated method stub
			return cp2VariableName;
		}

	public String getCP1Variable() {
			// TODO Auto-generated method stub
			return cp1VariableName;
		}
		  
	private  void initialzeOp() {
		
		logicalOpList = new ArrayList();
		logicalOpList.add("|");
		logicalOpList.add("&");
		
		relationalOpList.add("eq");
		relationalOpList.add("neq");
		relationalOpList.add("leq");
		relationalOpList.add("geq");
		relationalOpList.add("lt");
		relationalOpList.add("gt");		
		
	}

	private String getLogicalOp(String cond) {

	      String logicalOp = "";	
	      
	      int orPos = cond.indexOf(" | ");
	      int andPos = cond.indexOf(" & ");
			
	     while (orPos != andPos) {
	    	 
	    	  if(orPos < andPos){
	    		  
	    		  if(orPos == -1){
	    			  
	    			  logicalOp = logicalOp.concat(" & ");
	    			  cond = cond.substring(andPos+1);  
	    		  }
	    		  else{
	    	
	    			  logicalOp = logicalOp.concat(" | ");
	    			  cond = cond.substring(orPos+1);
	    		  }
	    		  
	    		  
	    	  }
	    	  else{
	    		  
	    		  if(andPos == -1){
	    			  
	    			  logicalOp = logicalOp.concat(" | ");
	    			  cond = cond.substring(orPos+1);
	    			}
	    		  else{
	    			  logicalOp = logicalOp.concat(" & ");
	    			  cond = cond.substring(andPos+1);
	    		  }
	    	  }
	    	  orPos = cond.indexOf(" | ");
			  andPos = cond.indexOf(" & ");
			   
	      }
			
			return logicalOp;
		}



		private ArrayList<String> findExpressions(String str) {
			
			ArrayList<String> expressions = new ArrayList();
			int flag = 0;
			
			int andPos, orPos, start = 0, end = str.length()-1;
			
			andPos = str.indexOf(" & ");
			orPos = str.indexOf(" | ");
			
			 while (orPos != andPos) {
		    	 
				 flag = 1;
				 
		    	  if (orPos < andPos) {
		    		  
		    		  if (orPos == -1) {
		    			
		    			  end = andPos;
		    			  expressions.add(str.substring(start, end));
		    			  str = str.substring(andPos + 1);  
		    		  
		    		  } else {
		    	
		    			  end = orPos;
		    			  expressions.add(str.substring(start, end));
		    			  str = str.substring(orPos + 1);  
		    		  }
		    		  
		    	  } else {
		    		  
		    		  if (andPos == -1) {
		    			 
		    			  end = orPos;
		    			  expressions.add(str.substring(start, end));
		    			  str = str.substring(orPos + 1);  

		    			} else {	
		    			  
		    				end = andPos;
		    				expressions.add(str.substring(start, end));
		    				str = str.substring(andPos + 1);  
		    		  }
		    	  }
		    	  
		    	  orPos = str.indexOf(" | ");
				  andPos = str.indexOf(" & ");
		      }
			
			 if (flag == 0)
				 expressions.add(str);
			
		/*	String str1= str;
			for(int i=0; i< str.length(); i++){
				
				if(logical_op.contains(str.charAt(i))){
					
					expressions.add(str1.substring(0, i));
					str1=str1.substring(i+1);
				}
			
			}*/
				
			return expressions;
		}
}
