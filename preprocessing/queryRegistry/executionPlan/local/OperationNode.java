package preprocessing.queryRegistry.executionPlan.local;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map.Entry;

public class OperationNode extends AndOrGraphNode implements java.io.Serializable {
//	private Integer id;
//	private Integer leftChild;
//	private Integer rightChild;
	private String operation;	//join type
	private String constraints;
	private String leftAttribute;
	private String rightAttribute;
//	private HashSet<EAGVertex> leftVertexSet;
//	private HashSet<EAGVertex> rightVertexSet;
	
	private LinkedList<EAGVertex> leftVertexSet;
	private LinkedList<EAGVertex> rightVertexSet;
	private HashMap<EAGVertex, String> leftOperandName;
	private HashMap<EAGVertex, String> rightOperandName;
	private HashMap<EAGVertex, HashSet<String>> leftNamingMap;
	private HashMap<EAGVertex, HashSet<String>> rightNamingMap;
	private ArrayList<String> operandList;
	private HashMap<EAGVertex, EAGVertex> leftNewVertex;
	private HashMap<EAGVertex, EAGVertex> rightNewVertex;

	public OperationNode(Integer id, Integer level) {
		super();
		setId(id);
		setLevel(level);
	//	this.leftVertexSet = new HashSet();
	//	this.rightVertexSet = new HashSet();
		
		this.leftVertexSet = new LinkedList();
		this.rightVertexSet = new LinkedList();
		this.leftNamingMap = new HashMap();
		this.rightNamingMap = new HashMap();
		this.leftOperandName = new HashMap();
		this.rightOperandName = new HashMap();
		this.operandList = new ArrayList();
		this.leftNewVertex = new HashMap();
		this.rightNewVertex = new HashMap();
		
	}
	
	/*
	 * Setters
	 */
		
	public void addNewLVertex(EAGVertex key, EAGVertex value) {
		this.leftNewVertex.put(key, value);
		
	}

	public void addNewRVertex(EAGVertex key, EAGVertex value) {
		this.rightNewVertex.put(key, value);
		
	}
	
	public void addOperands(String op) {
		operandList.add(op);
	}
	
	public ArrayList<String> getOperandList () {
		return this.operandList;
	}
	
	public void setOperation(String op) {
		this.operation = op;
	}
	
	public void setLeftAttribute(String lAttr) {
		this.leftAttribute = lAttr;
	}
	
	public void setRightAttribute(String rAttr) {
		this.rightAttribute = rAttr;
	}
	
	public void setCondtraint(String constraint) {
		this.constraints = constraints;
	}
	
	public void addLeftEAVertex(EAGVertex v) {
		//EAGVertex leftVertex = new EAGVertex(id, form);
		leftVertexSet.add(v);
	}
	
	public void addRightEAVertex(EAGVertex v) {
		//EAGVertex rightVertex = new EAGVertex(id, form);
		rightVertexSet.add(v);
	}
		
	/*
	 * Getters
	 */
	public EAGVertex getLeftNewVertex (EAGVertex key) {
		return this.leftNewVertex.get(key);
	}
	
	public EAGVertex getRightNewVertex (EAGVertex key) {
		return this.rightNewVertex.get(key);
	}
	
	public String getOperator() {
		return operation;
	}
	
/*	public HashSet<EAGVertex> getLeftOperand() {
		return leftVertexSet;
	}
	
	public HashSet<EAGVertex> getRightOperand() {
		return rightVertexSet;
	}
*/	
	public LinkedList<EAGVertex> getLeftOperand() {
		return leftVertexSet;
	}
	
	public LinkedList<EAGVertex> getRightOperand() {
		return rightVertexSet;
	}

	public void addLeftNamingEntry(EAGVertex v, String str) {
		
		if (leftNamingMap.containsKey(v)) {
			leftNamingMap.get(v).add(str);
		} else {
			
			HashSet<String> temp =new HashSet();
			temp.add(str);
			leftNamingMap.put(v, temp);
		}
		
		//this.namingMap.put(v, str);
	}
	
	public void addLeftNamingEntry(EAGVertex v, HashSet<String> str) {
		
	//	System.out.println("v is "+v.getId()+"\t"+v.getForm()+"\t"+v.getNeighborList().toString());
	//	System.out.println("Here is info "+this.id+":\n");
		/*if(str==null) {
			
			for(Entry<EAGVertex,EAGVertex> entry: this.leftNewVertex.entrySet()) {
				System.out.println("\nL: "+entry.getKey().getId()+" :"+entry.getKey().toString()+"\t"+entry.getValue().getId()+" :"+entry.getValue().toString()+"\n");
			}
			System.out.println("Left Operand: "+this.getLeftOperand().get(0).toString()+"\t"+ this.getLeftOperand().get(0).getId()+"\t"+this.getLeftOperand().size());
		
		
		for(Entry<EAGVertex,EAGVertex> entry: this.rightNewVertex.entrySet()) {
			System.out.println("\nR: "+entry.getKey().getId()+" :"+entry.getKey().toString()+"\t"+entry.getValue().getId()+" :"+entry.getValue().toString()+"\n");
		}
		System.out.println("right Operand: "+this.getRightOperand().get(0).toString()+"\t"+ this.getRightOperand().get(0).getId()+"\t"+this.getRightOperand().size());
		}*/
		
		//System.out.println("\nChecking value of name mapping: "+str.size());
		
		if (leftNamingMap.containsKey(v)) {
	//		System.out.println("LIf: Here is some problem!!"+leftNamingMap.get(v));
			leftNamingMap.get(v).addAll(str);
		} else {

			leftNamingMap.put(v, str);
	//		System.out.println("LElse: Here is some problem!!"+leftNamingMap.get(v));

		}
		
		//this.namingMap.put(v, str);
	}

	
	public HashMap<EAGVertex, EAGVertex> getLeftVertexMap() {
		return this.leftNewVertex;
	}
	
	public HashMap<EAGVertex, EAGVertex> getRightVertexMap() {
		return this.rightNewVertex;
	}

	
	public void addAllLeftNamingEntry(HashMap<EAGVertex , HashSet<String>> map) {
		
		if (leftNamingMap.isEmpty()) {
			this.leftNamingMap.putAll(map);
		
		} else {
			for (Entry<EAGVertex, HashSet<String>> entry : map.entrySet()) {
				
				 if (leftNamingMap.containsKey(entry.getKey())) {
					 
					 leftNamingMap.get(entry.getKey()).addAll(entry.getValue());
					 
				 } else {
					 leftNamingMap.put(entry.getKey(), entry.getValue());
				 }
				
			}
		}
		
	}

	public HashSet<String> getNameOfEAG (EAGVertex key) {
		/*
		 * Check both the left and right naming map and add both the string in the naming
		 * 
		 */
		HashSet<String> result = new HashSet();
		
		if (leftNamingMap.containsKey(key)) {
			result.addAll(leftNamingMap.get(key));
		}
		
		if (rightNamingMap.containsKey(key)) {
			result.addAll(rightNamingMap.get(key));
		}
		
		return result;
		
		/*
		if (leftNamingMap.containsKey(key)) {
		//	System.out.println("LEft naming problem: "+leftNamingMap.get(key));
			return this.leftNamingMap.get(key);
		} else {
			
			if(rightNamingMap.containsKey(key)) {
				
			}
		  //	System.out.println("Right naming problem: "+rightNamingMap.get(key));
			else{
				
				
					for(Entry<EAGVertex,EAGVertex> entry: this.leftNewVertex.entrySet()) {
					//	System.out.println("\nL: "+entry.getKey().getId()+" :"+entry.getKey().toString()+"\t"+entry.getValue().getId()+" :"+entry.getValue().toString()+"\n");
					}
					//System.out.println("Left Operand: "+this.getLeftOperand().get(0).toString()+"\t"+ this.getLeftOperand().get(0).getId()+"\t"+this.getLeftOperand().size());


					for(Entry<EAGVertex,EAGVertex> entry: this.rightNewVertex.entrySet()) {
				//		System.out.println("\nR: "+entry.getKey().getId()+" :"+entry.getKey().toString()+"\t"+entry.getValue().getId()+" :"+entry.getValue().toString()+"\n");
						}
				//	System.out.println("right Operand: "+this.getRightOperand().get(0).toString()+"\t"+ this.getRightOperand().get(0).getId()+"\t"+this.getRightOperand().size());
			}

				
		    //System.out.println("Rigth: NO entry at all "+rightNamingMap.get(key).size());
			return this.rightNamingMap.get(key);
		}
	*/	
	}
	
	public HashMap<EAGVertex, HashSet<String>> getLeftNamingMap() {
		return this.leftNamingMap;
	}

	
	
	public void addRightNamingEntry(EAGVertex v, String str) {
		
		if (rightNamingMap.containsKey(v)) {
			rightNamingMap.get(v).add(str);
		} else {
			
			HashSet<String> temp =new HashSet();
			temp.add(str);
			rightNamingMap.put(v, temp);
		}
		
		//this.namingMap.put(v, str);
	}
	
	public void addRightNamingEntry(EAGVertex v, HashSet<String> str) {
		//System.out.println("v is "+v.getId()+"\t"+v.getForm()+"\t"+v.getNeighborList().toString());
	//	System.out.println("Here is info "+this.id+":\n");
	/*	if(str==null) {
						
			for(Entry<EAGVertex,EAGVertex> entry: this.leftNewVertex.entrySet()) {
				System.out.println("\nL: "+entry.getKey().getId()+" :"+entry.getKey().toString()+"\t"+entry.getValue().getId()+" :"+entry.getValue().toString()+"\n");
			}
			System.out.println("Left Operand: "+this.getLeftOperand().get(0).toString()+"\t"+ this.getLeftOperand().get(0).getId()+"\t"+this.getLeftOperand().size());
		
		
		for(Entry<EAGVertex,EAGVertex> entry: this.rightNewVertex.entrySet()) {
			System.out.println("\nR: "+entry.getKey().getId()+" :"+entry.getKey().toString()+"\t"+entry.getValue().getId()+" :"+entry.getValue().toString()+"\n");
		}
		System.out.println("right Operand: "+this.getRightOperand().get(0).toString()+"\t"+ this.getRightOperand().get(0).getId()+"\t"+this.getRightOperand().size());
		}
		
		*/
	//	System.out.println("\nChecking value of name mapping: "+str.size());

		if (rightNamingMap.containsKey(v)) {
	//		System.out.println("RIf: Here is some problem!!"+rightNamingMap.get(v));

			rightNamingMap.get(v).addAll(str);
		} else {
			
			rightNamingMap.put(v, str);
	//		System.out.println("RElse: Here is some problem!!"+rightNamingMap.get(v));

		}
		
		//this.namingMap.put(v, str);
	}
	
public void addAllRightNamingEntry(HashMap<EAGVertex , HashSet<String>> map) {
		
		if (rightNamingMap.isEmpty()) {
			this.rightNamingMap.putAll(map);
		
		} else {
			for (Entry<EAGVertex, HashSet<String>> entry : map.entrySet()) {
				
				 if (rightNamingMap.containsKey(entry.getKey())) {
					 
					 rightNamingMap.get(entry.getKey()).addAll(entry.getValue());
					 
				 } else {
					 rightNamingMap.put(entry.getKey(), entry.getValue());
				 }
				
			}
		}
		
	}

/*	public HashSet<String> getNameOfRightEAG (EAGVertex key) {
		return this.rightNamingMap.get(key);
	}
	*/
	public HashMap<EAGVertex, HashSet<String>> getRightNamingMap() {
		return this.rightNamingMap;
	}

}
