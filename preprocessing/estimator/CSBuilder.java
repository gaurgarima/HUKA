package preprocessing.estimator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Set;

import main.ConfigurationReader;

public class CSBuilder {

	public static void main(String[] args) throws ClassNotFoundException, IOException {
		
		ConfigurationReader.readConfiguration();
		boolean isFirst = true;

		for (String fileName : args) {

			buildEntitymMap(fileName, isFirst);

			if (isFirst)
				isFirst = false;
		}

		characterisiticSetBuilder();
		csPairBuilder();
	
	}
	
	static void buildEntitymMap(String inputFileName, boolean isFirstTime) throws IOException, ClassNotFoundException {
		
		LinkedHashMap<String, LinkedHashMap<String, Predicate>> entityMap = new LinkedHashMap();
		String outputFileName = ConfigurationReader.get("ENTITY_MAP");
		String inputFile = ConfigurationReader.get("CSV_FILE");
				
		if (!isFirstTime) {
			
			FileInputStream fi = new FileInputStream(new File(outputFileName));
			ObjectInputStream oi = new ObjectInputStream(fi);
			entityMap = (LinkedHashMap<String, LinkedHashMap<String, Predicate>>) oi.readObject();
			oi.close();
			fi.close();
		}
	
		BufferedReader br = new BufferedReader(new FileReader(inputFile+inputFileName));
		String line, sub, pred, obj;
		long line_num = 0;
	
		while((line = br.readLine()) != null){
		  
			line_num++;
		
			String[] line_comp = line.split("\t");
			sub = line_comp[0];
			pred = line_comp[1];
			obj = line_comp[2];
	
			 		
			if(entityMap.containsKey(sub))
			{
				if(entityMap.get(sub).containsKey(pred)){
				
					entityMap.get(sub).get(pred).add_obj(obj);
					entityMap.get(sub).get(pred).incr_count();
				}
				else{
					entityMap.get(sub).put(pred, new Predicate(pred, new LinkedHashSet(Arrays.asList(obj)),1));
				}
			}
			else{			
				LinkedHashMap<String, Predicate> map = new LinkedHashMap();
				map.put(pred, new Predicate(pred, new LinkedHashSet(Arrays.asList(obj)),1));
				entityMap.put(sub, map);
			}
		
		}
	
		br.close();
		
		FileOutputStream fos = new FileOutputStream(outputFileName);
		ObjectOutputStream oos = new ObjectOutputStream(fos);
		oos.writeObject(entityMap);
		oos.close();
		fos.close();

	}


	static void characterisiticSetBuilder() throws ClassNotFoundException, IOException{
		
		/*
		 * Read the entity_map and get cs out of it plus 
		 */
		LinkedHashMap<String, LinkedHashMap<String, Predicate>> entityMap = new LinkedHashMap();
		String entityMapPath = ConfigurationReader.get("ENTITY_MAP");
		FileInputStream fi = new FileInputStream(new File(entityMapPath));
		ObjectInputStream oi = new ObjectInputStream(fi);
	
		entityMap= (LinkedHashMap<String, LinkedHashMap<String, Predicate>>) oi.readObject();
		oi.close();
		fi.close();
	
		LinkedHashMap<HashSet<String>, LinkedHashMap<String, ModifiedPredicate>> CS = new LinkedHashMap();
		LinkedHashMap<HashSet<String>, Integer> cs_distinct = new LinkedHashMap();

		
		for(String entity: entityMap.keySet()){

			HashSet<String> cs_key = new HashSet();
			cs_key.addAll(entityMap.get(entity).keySet());
		
			if(CS.keySet().contains(cs_key)){
				
				LinkedHashMap<String, ModifiedPredicate> map = new LinkedHashMap(CS.get(cs_key));
				
				for(String pred: map.keySet()){
					
					Predicate p = new Predicate(entityMap.get(entity).get(pred));
					map.get(pred).update_counter(p.get_count());
					map.get(pred).merge_objSet(p.get_ObjSet());
				}

				CS.put(cs_key, map);
				int count = cs_distinct.get(cs_key);
				cs_distinct.put(cs_key, count+1);
			}
			else{
				
				LinkedHashMap<String, ModifiedPredicate> map = new LinkedHashMap();
				
				for(String pred: entityMap.get(entity).keySet()){
					
					ModifiedPredicate p = new ModifiedPredicate(entityMap.get(entity).get(pred));
					map.put(pred, p);
				}
				
				CS.put(cs_key, map);
				cs_distinct.put(cs_key, 1);

			}
			
		}
		
		String csPath = ConfigurationReader.get("CS");
		String csCountPath = ConfigurationReader.get("CS_COUNT");
		
		FileOutputStream fos = new FileOutputStream(csPath);
		ObjectOutputStream oos = new ObjectOutputStream(fos);
		oos.writeObject(CS);
		oos.close();
		fos.close();
		
		FileOutputStream fos1 = new FileOutputStream(csCountPath);
		ObjectOutputStream oos1 = new ObjectOutputStream(fos1);
		oos1.writeObject(cs_distinct);
		oos1.close();
		fos1.close();
	}

		
	static void csPairBuilder() throws IOException, ClassNotFoundException{
		
		/*
		 *	pair -- cs_i.toString()+":"+ cs_j.toString()  
		 */
		
		LinkedHashMap<LinkedList<HashSet<String>>, LinkedHashMap<String,Integer>> csPair = new LinkedHashMap();
		LinkedHashMap<LinkedList<HashSet<String>>,Integer> csPairCount = new LinkedHashMap();
		LinkedHashMap<String, LinkedHashMap<String, Predicate>> entityMap = new LinkedHashMap();

		String entityMapPath = ConfigurationReader.get("ENTITY_MAP");
		FileInputStream fi = new FileInputStream(new File(entityMapPath));

		ObjectInputStream oi = new ObjectInputStream(fi);
		entityMap= (LinkedHashMap<String, LinkedHashMap<String, Predicate>>) oi.readObject();
		oi.close();
		fi.close();
		
		for(String sub: entityMap.keySet()){
			
			LinkedHashMap<String, Predicate> entry = new LinkedHashMap(entityMap.get(sub));
			HashSet<String> CS_i_identifier = new HashSet();
			CS_i_identifier.addAll(entry.keySet());
			
			HashSet<String> flagSet = new LinkedHashSet();
			
			for(String pred: entry.keySet()){
				
				int flag = 0;
				Set<String> neighbors = new LinkedHashSet(entry.get(pred).get_ObjSet());
					
				for(String obj: neighbors) {
					
					flag++;
					HashSet<String> CS_j_identifier = new HashSet();
					
					if(entityMap.containsKey(obj))
					CS_j_identifier.addAll(entityMap.get(obj).keySet());
					
					if(!CS_j_identifier.isEmpty()) {
						
						LinkedList<HashSet<String>> csPairIdentifier = new LinkedList();
						csPairIdentifier.add(CS_i_identifier);
						csPairIdentifier.add(CS_j_identifier);
						
						if(csPair.containsKey(csPairIdentifier)) {
							
							if(csPair.get(csPairIdentifier).keySet().contains(pred)) {
								
								int count = csPair.get(csPairIdentifier).get(pred);
				
								if(flag < 2){
								
									int distcount = csPairCount.get(csPairIdentifier);
									distcount++;
									csPairCount.put(csPairIdentifier, distcount);
								}

								count++;
								csPair.get(csPairIdentifier).put(pred, count);
							
							} else{
								
								csPair.get(csPairIdentifier).put(pred, 1);
								int distcount = csPairCount.get(csPairIdentifier);	
								distcount++;
								csPairCount.put(csPairIdentifier, distcount);
							}

						} else{
							
							LinkedHashMap<String, Integer> map = new LinkedHashMap();
							map.put(pred, 1);
							csPair.put(csPairIdentifier, map);
							csPairCount.put(csPairIdentifier, 1);
							
						}
					}	//	if neighbor is a star
				}	// after all neighbors of a pred are checked
			}		// after all pred of a subject are checked   
		}	// complete entity map is checked
				
		int max = 0, count = 0;
		LinkedList<HashSet<String>> s = new LinkedList();

		for(LinkedList<HashSet<String>> str: csPairCount.keySet()){
			
			int c = csPairCount.get(str);

			if(c > max) {
				max = c;
				s = str;
			}

			if(c > 100)
				count++;
		}
		
		String csPairPath = ConfigurationReader.get("CS_PAIR");
		String csPairCountPath = ConfigurationReader.get("CS_PAIR_COUNT");

		FileOutputStream fos = new FileOutputStream(csPairPath);
		ObjectOutputStream oos = new ObjectOutputStream(fos);
		oos.writeObject(csPair);
		oos.close();
		fos.close();
		
		FileOutputStream fos1 = new FileOutputStream(csPairCountPath);
		ObjectOutputStream oos1 = new ObjectOutputStream(fos1);
		oos1.writeObject(csPairCount);
		oos1.close();
		fos1.close();

				/*
		System.out.println("Total characterisitc set (CS) pairs: "+csPair.size());
		System.out.println("Largest CS pair: "+max+"\t"+count);
		System.out.println("Most common CS pair"+s.toString());
		*/

	
	}
	
	
}
