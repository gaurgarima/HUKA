package maintenance.deletion;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import main.ConfigurationReader;

public class ResultStore {
	
	String queryId = new String();
	HashMap<String,String> resultSet=new HashMap<String,String>();
	
	
	public ResultStore(String qid){
		
		queryId = qid;
	}

	public void load() throws IOException{
	 
		String resultFile = ConfigurationReader.get("QUERY_RESULT_FILE");
		BufferedReader br = new BufferedReader(new FileReader(resultFile+"Q_"+queryId+".txt"));

		String line = new String();
		String[] line_comp;
		
		while ((line = br.readLine())!= null) {
			
			line_comp = line.split("\t");
			resultSet.put(line_comp[0],line_comp[1]);
		}
		br.close();
		
	}
	
	public String getQueryId(){
		
		return queryId;
	}
	
	public int hasChanged(String edge_id, ArrayList<String> itemList){
		
		String poly = new String();
		String[] addends;
		int flag = 0;
		int hasChanged = 0;
		Set<String> candidateList = new HashSet(itemList);
		
/*
		item_list=item_list.substring(1,item_list.length()-1);
		String comp[]=item_list.split("}, ");
		
		if(comp.length> 1)
			comp[0]=comp[0].concat("}");
		
		
		if(comp.length >2){
			
			for(int i=1;i<comp.length-1;i++){
				comp[i]=comp[i].concat("}");
			}
		}
		
		for(String c: comp){
		
			candidate_item.add(c);
		}
*/

		//candidate_item.putAll(item_list);

		for(String key: candidateList){
			
			String updatedPoly = new String();
			updatedPoly = "";
			flag = 0;
			
			if(resultSet.containsKey(key)){
				
				poly = resultSet.get(key);
				addends = poly.split("\\+");
			
				for (String addend : addends) {
				
					List<String> ids = new ArrayList<String>(Arrays.asList(addend.split("e")));
				
					/*
					 * Temp solution
					 */
					List<String>idsTemp = new ArrayList();
					idsTemp.addAll(ids);
					ids.clear();
							
					for (String i: idsTemp) {
						if(i.endsWith("."))
						i = i.substring(0,i.length()-1);
						
						ids.add(i);
					}
					
					String e = edge_id.replace("e","");
					if(ids.contains(e))
						flag++;	
					else	
						updatedPoly = updatedPoly.concat(addend+"+");
				} 
			
				if (flag == addends.length) {			
				
					resultSet.remove(key);
					hasChanged = hasChanged+1;
				}	
				else if(flag > 0) {
					
					updatedPoly = updatedPoly.substring(0,updatedPoly.length()-1);
					resultSet.put(key,updatedPoly);
				}
			}
		}

		updateResultStore();
		return hasChanged;
	}
	
	void updateResultStore(){
		
		FileWriter fw = null;
		File file;
		String resultFile = ConfigurationReader.get("QUERY_RESULT_FILE");

		try {

			file = new File(resultFile+"Q_"+queryId+".txt");
			fw = new FileWriter(file,false);

			if (!file.exists()) 
				file.createNewFile();
			
			
			for(String item : resultSet.keySet())
				fw.write(item+"\t"+resultSet.get(item)+"\n");
						
			fw.close();
       }catch(Exception e){}
	} 
 
}