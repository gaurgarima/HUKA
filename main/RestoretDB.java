package main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class RestoretDB {

	public static void main(String[] args) throws SQLException, IOException {
		
		String username = "root";
		String passwrd = "";
		
	Connection	connect = DriverManager
                .getConnection("jdbc:mysql://localhost/DBPedia?"
                        + "user="+username+"&password="+passwrd);
	
	
	connect.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

	BufferedReader br;
	String setting = "5-5-1";
	br = new BufferedReader( new FileReader( new File("./experiment/correct/dbpedia/toDelete_"+setting+".txt")));
	String line = "";
	String edgeId = "";
	String relation = "";
	
	String subject = new String();
	String object = new String();
	
	String qStmt = "";
	int i = 0;
	
	
	while ( (line = br.readLine()) != null) {
		
		String lineComp[] = line.split("\t");
		subject = lineComp[1];
		object = lineComp[2];
		edgeId = lineComp[3];
		relation = lineComp[0];
	
	 	PreparedStatement preparedStatement = connect.prepareStatement("Delete from  `"+relation+"` where `out` = ? and `in` = ? and poly = ?");

//		PreparedStatement preparedStatement = connect.prepareStatement("insert into  `"+relation+"` values (?, ?, ?)");
   	 
		preparedStatement.setString(1, subject);		
        preparedStatement.setString(2, object);		
        preparedStatement.setString(3, "e"+edgeId);		

        preparedStatement.executeUpdate();

        i++;
        System.out.println("D"+i);
	
	}
	
	br.close();
	
	System.out.println("Deletion Complete");

	i = 0;
	
	br = new BufferedReader( new FileReader( new File("./experiment/correct/dbpedia/toAdd_"+setting+".txt")));
		
	while ( (line = br.readLine()) != null) {
		
		String lineComp[] = line.split("\t");
		subject = lineComp[1];
		object = lineComp[2];
		edgeId = lineComp[3];
		relation = lineComp[0];
	
	// 	PreparedStatement preparedStatement = connect.prepareStatement("Delete from  `"+relation+"` where `out` = ? and `in` = ? and poly = ?");

		PreparedStatement preparedStatement = connect.prepareStatement("insert into  `"+relation+"` values (?, ?, ?)");
   	 
		preparedStatement.setString(1, subject);		
        preparedStatement.setString(2, object);		
        preparedStatement.setString(3, "e"+edgeId);		

        preparedStatement.executeUpdate();


        i++;
        System.out.println("I"+i);

	
	}
	br.close();
	 			
	System.out.println("Insertion Complete");
	connect.close();
	
	}

}
