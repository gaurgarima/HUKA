

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.HashMap;

import main.ConfigurationReader;

public class Neo4JImport {

	public static void main(String[] args) throws IOException, FileNotFoundException {
		// TODO Auto-generated method stub
		ConfigurationReader.readConfiguration();
		//createCompleteFile();
	}

	private static void createCompleteFile() throws IOException, FileNotFoundException {

		File entityFile = new File("./entityFile.txt");
		
		File factFile = new File("./factFile.txt");
		File predFile = new File("./relationFile.txt");
		
		
		File completeFile = new File("./factsWithId.txt");
		
		if(!completeFile.exists())
			completeFile.createNewFile();
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(completeFile));
		BufferedReader brEntity = new BufferedReader(new FileReader(entityFile));
		BufferedReader brFact = new BufferedReader(new FileReader(factFile));
		BufferedReader brPred = new BufferedReader(new FileReader(predFile));

		HashMap<String, String> entityMap = new HashMap();
		HashMap<String, String> predicateMap = new HashMap();

		String line = "";
		String lineComp[] = null;
		
		while ((line = brEntity.readLine()) != null) {
			lineComp = line.split("\t");
			entityMap.put(lineComp[1], lineComp[0]);
		}
		brEntity.close();
		
		while ((line = brPred.readLine()) != null) {
			lineComp = line.split("\t");
			predicateMap.put(lineComp[0], lineComp[1]);
		}
		brPred.close();
		System.out.println("Entity: "+ entityMap.size()+"\nPred: "+predicateMap.size());
		
		String subId = "";
		String objId = "";
		String edgeId = "";
		String predicate = "";
		String predURI = "";
		
		while ((line = brFact.readLine()) != null) {
			lineComp = line.split("\t");
			
			predURI = lineComp[1];
			edgeId = lineComp[3];
			
			if (entityMap.containsKey(lineComp[0]))
				subId = entityMap.get(lineComp[0]);
			
			if (entityMap.containsKey(lineComp[2]))
				objId = entityMap.get(lineComp[2]);
			
			if (predicateMap.containsKey(predURI))
				predicate = predicateMap.get(predURI);
			
			bw.write(subId+"\t"+edgeId+"\t"+objId+"\t"+predicate+"\n");
			
		}
		
		bw.close();

	}
	
	
	

}
