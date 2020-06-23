package preprocessing.queryRegistry.executionPlan.local;

/**
 * This class represents a SPARQL query triple.
 * A set of triple patterns will form the AbstractGraph eventaully (not sure about this)
 * @author garima
 *
 */
public class TriplePattern  implements java.io.Serializable{
  private Integer tripleId;	
  private Entity subjectEntity;
  private Entity relationEntity;
  private Entity objectEntity;
  private String triplePatternValue;
  private String triplePatternAbstractForm;
	
  public TriplePattern (Integer id, String subject, String relation, String object) {
	  
	  String subjectType = "const";
	  String objectType = "const";
	  String relationType = "const";
	  
	  if (subject.startsWith("?")) {
		  subjectType = "var";  
	  } else {
		  subjectType = subjectType.concat(" - "+subject);
	  }
	  
	  if (object.startsWith("?")) {
		  objectType = "var";
	  } else {
		  objectType = objectType.concat(" - "+object);
	  }
	  
	  if (relation.startsWith("?")) {
		  relationType = "var";
	  }
	  
	  tripleId = id;
	  subjectEntity = new Entity("subject", subject, subjectType);
	  objectEntity = new Entity("object", object, objectType);
	  relationEntity = new Entity("relation", relation, relationType);
	  triplePatternValue = subject.concat(", ").concat(relation).concat(", ").concat(object);
	  
	  if (relationType.equals("const")) {
		  triplePatternAbstractForm = subjectType.concat(", ").concat(relation).concat(", ").concat(objectType);  
	  } else {
		  triplePatternAbstractForm = subjectType.concat(", ").concat(relationType).concat(", ").concat(objectType);
	  }
	  
	//  System.out.println(triplePatternValue+"\t"+triplePatternAbstractForm);
  }
  
  public String getSubject() {
	  return subjectEntity.getValue();
  }
  
  public String getObject() {
	  return objectEntity.getValue();
  }
  
  public String getSubjectType() {
	  return subjectEntity.getValueType();
  }
  
  public String getObjectType() {
	  return objectEntity.getValueType();
  }
  
  public String getRelation() {
	  return relationEntity.getValue();
  }
  
  public String getAbstractForm() {
	  return triplePatternAbstractForm;
  }
  
  public String getTriplePattern(){
	  return triplePatternValue;
  }
  
  public Integer getId() {
	  return tripleId;
  }
}
