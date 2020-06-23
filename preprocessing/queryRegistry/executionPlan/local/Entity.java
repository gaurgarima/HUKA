package preprocessing.queryRegistry.executionPlan.local;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * This class is used to store the entities involved -- subject, object and relation 
 * in a triple
 * @author garima
 *
 */
public class Entity implements java.io.Serializable {
	
	/**
	 * entityType -- subject, object or relation
	 * value -- the variable or constant which begin used to represent this entity in SPARQL query
	 * valuetype -- var or const
	 */
  private String entityType;
  private String value;
  private String valueType;
  
  public Entity(){
	  entityType = new String();
	  value = new String();
	  valueType = new String();
  }
  
  public Entity(String entityType, String value, String vType){
	setEntityType(entityType);
	setValue(value);
	setValueType(vType);
  }
  
  final public String getEntitytype(){
    return this.entityType;
  }
  
  final public String getValue(){
    return this.value;
  }
  
  final public String getValueType(){
	  return this.valueType;
  }
  
  final public void setEntityType(String type){
	  if (new ArrayList(Arrays.asList("subject", "relation", "object")).contains(type)){
		  this.valueType = type;
	  } else {
		  System.out.println("Invalid Entity type");
	  }
	  
  }
  
  final public void setValue(String value){
	  this.value = value;
  }
  
  final public void setValueType(String vType){
	  this.valueType = vType;
  }
}
