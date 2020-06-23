package maintenance.deletion;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


public class UpdatePropagator {

	static int bit = 1 ;
	List<String> query_id_list;		//List of queries 
	Map<String,ResultStore> resultStoreList = new HashMap<String,ResultStore>();
	static HashMap<String,HashMap<String, ArrayList<String>>> invertedIndex = new HashMap();
	static HashMap<String,HashSet<String>> edgeToCPMap = new HashMap();
	
	public UpdatePropagator(List<String> ids) throws IOException{

	    query_id_list=new ArrayList(ids);	
	
		for(String id: query_id_list)
			resultStoreList.put(id,new ResultStore(id));
				
		for(ResultStore res: resultStoreList.values())
			res.load();
	}
	
	public Map<String,Integer> deleteEdge(String edge_id, HashMap<String, ArrayList<String>> resultItemList) throws IOException{

		HashMap<String, Integer> affectedQueryMap = new HashMap();
		
		for (String queryId : resultStoreList.keySet()) {
			
			int flag = 0;
			
			flag = resultStoreList.get(queryId).hasChanged(edge_id, resultItemList.get(queryId));
			
			if (flag > 0) {
				affectedQueryMap.put(queryId, flag);
			}
		}
			
		return affectedQueryMap;
	}

	
	
}
