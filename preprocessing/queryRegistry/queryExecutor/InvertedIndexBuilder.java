package preprocessing.queryRegistry.queryExecutor;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import main.GlobalDSReader;

import java.util.Properties;
import java.util.Set;


public class InvertedIndexBuilder {


	
	public void buildIndex(HashMap<String, String> result, String qId) {
		
		HashMap<String,HashMap<String,ArrayList<String>>> invertedIndex = new HashMap();
		invertedIndex.putAll(GlobalDSReader.getInvertedIndex());
		HashMap<String,HashMap<String,ArrayList<String>>> tempInvertedIndex = new HashMap();
		
		String item = new String();
		String polyProv = new String();
		
		for (Entry<String, String> entry: result.entrySet()) {
			
			item = entry.getKey();
			polyProv = entry.getValue();
			HashSet<String> edgeid_set = new HashSet();
		
			if (polyProv.contains("+")) {

				String terms[] = polyProv.split("\\+");
				
				for(String term: terms) {
					
					String edge_comp[] = term.split("e");
					
					for(int i = 1; i < edge_comp.length; i++)
						edgeid_set.add("e"+edge_comp[i]);
				}									
			} else{
				
				String edge_comp[] = polyProv.split("e");
			
				for(int i = 1; i < edge_comp.length; i++)
					edgeid_set.add("e"+edge_comp[i]);
			}

			for(String e_id: edgeid_set){

				if (e_id.endsWith("."))
					e_id = e_id.substring(0,e_id.length()-1);
				
				if(invertedIndex.containsKey(e_id)){

					if(invertedIndex.get(e_id).containsKey(qId)) {
					
						invertedIndex.get(e_id).get(qId).add(item);
					} else{
					
						ArrayList<String> item_list= new ArrayList();
						item_list.add(item);
						invertedIndex.get(e_id).put(qId,item_list);
					}
				
				} else{

					ArrayList<String> item_list= new ArrayList();
					item_list.add(item);
					HashMap<String, ArrayList<String>> qidtoitems= new HashMap();
					qidtoitems.put(qId,item_list);
					invertedIndex.put(e_id, qidtoitems);
				}

				tempInvertedIndex.put(e_id, invertedIndex.get(e_id));
			} 
		}
		
		GlobalDSReader.addToInvertedIndex(tempInvertedIndex);
	}
}
