/**
 *****************************************************************************************************************
 * File Name     : DBComparator
 * Description   : service class to connect to  the DB's and compare  
 * Project       : DB Comparison Tool
 * Package       : com.igate.dbc.executor
 * Author        : Ravi Kishore Medarmitla
 * Date			 : Aug 17, 2015
 *
 * ****************************************************************************************************************
 */

package com.igate.dbc.service;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;

public class DBComparatorService {

	private static Connection devConn = null;
	private static Connection qaConn = null;
	private static Properties prop = null;
	private static InputStream input = null;
	private static String devSchema = null;
	private static String qaSchema = null;
	private static ResultSet devTables, qaTables,devColumns,qaColumns,devFkNames,qaFkNames ,fkDetailsSet= null;
	private static PreparedStatement psmt = null;
	private static ArrayList<String> missingTables = null;
	private static ArrayList<String> missingColumns = null;
	private static ArrayList<String> missingFkNames = null;
	private static String SUPPORT = "SUPPORT";
	private static String GRNHILLS = "GRNHILLS";
	private static String tableQuery = "SELECT TABLE_NAME FROM SYSIBM.TABLES WHERE TABLE_SCHEMA = ?";
	private static String columnQuery = "SELECT COLUMN_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH FROM SYSIBM.COLUMNS WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ?";
	private static String fkQuery="select FK_NAME from SYSIBM.SQLFOREIGNKEYS WHERE PKTABLE_SCHEM=? AND FKTABLE_SCHEM=?";
	private static String fkDetails="select PKTABLE_NAME,PKCOLUMN_NAME,FKTABLE_NAME,FKCOLUMN_NAME from SYSIBM.SQLFOREIGNKEYS WHERE PKTABLE_SCHEM='SMC' AND FKTABLE_SCHEM='SMC' AND FK_NAME=?";
	private static String SEPARATOR="  -  ";



	/**
	 * Method to connect to the DB's
	 * 
	 */
	public  Connection getDBConnection(String driver, String dataBaseURL,
			String username, String password) throws ClassNotFoundException,
			SQLException {
		Connection con = null;
		Class.forName(driver);
		con = (Connection) DriverManager.getConnection(dataBaseURL, username,
				password);
		if (con.getCatalog().equalsIgnoreCase(SUPPORT)) {
			System.out.println("Connected to Dev(Support) Server");
		} else if (con.getCatalog().equalsIgnoreCase(GRNHILLS)) {
			System.out.println("Connected to GrnHills(QA)");
		}
		return con;
	}

	//connection to first database 
	public  void connectToDb1() throws IOException,
	SQLException, ClassNotFoundException {
		prop = new Properties();
		input = new FileInputStream("config.properties");
		prop.load(input);
		String driver = prop.getProperty("tool-dbdriver1");
		String dataBaseURL = prop.getProperty("tool-dbanme1");
		String username = prop.getProperty("tool-dbusername1");
		String password = prop.getProperty("tool-dbpassword1");
		devSchema = prop.getProperty("tool-schema1");
		devConn = getDBConnection(driver, dataBaseURL, username, password);
	}
	// Connection to second database.
	public  void connectToDb2() throws IOException,
	ClassNotFoundException, SQLException {
		prop = new Properties();
		input = new FileInputStream("config.properties");
		prop.load(input);
		String driver = prop.getProperty("tool-dbdriver2");
		String dataBaseURL = prop.getProperty("tool-dbanme2");
		String username = prop.getProperty("tool-dbusername2");
		String password = prop.getProperty("tool-dbpassword2");
		qaSchema = prop.getProperty("tool-schema2");
		qaConn = getDBConnection(driver, dataBaseURL, username, password);
	}

	//compare DB's
	public void compareDB(BufferedWriter out) throws SQLException, IOException{


		String tableName=null;
		StringBuffer devColumnDetails=null;
		StringBuffer qaColumnDetails=null;

		out.write("Comparing  Schema:"+qaSchema+" DBName:"+prop.getProperty("tool-dbanme2")+" over Schema:"+devSchema+" DBName"+prop.getProperty("tool-dbanme1")   );
		out.newLine();
		out.write("______________________________________________________________________________________________________");
		out.newLine();
		out.newLine();
		out.newLine();
		psmt = devConn.prepareStatement(tableQuery);
		psmt.setString(1, devSchema);
		devTables = psmt.executeQuery();
		System.out.println("Reading Tables In Dev(Support)");

		psmt = qaConn.prepareStatement(tableQuery);
		psmt.setString(1, qaSchema);
		qaTables = psmt.executeQuery();
		System.out.println("Reading Tables In GrnHills(QA)");

		missingTables = new ArrayList<String>();

		while (devTables.next()) {


			missingTables.add(devTables.getString(1));

		}
		System.out
		.println("Comparing Dev(Support) Over GrnHills(QA) Please Wait.........");
		while (qaTables.next()) {

			tableName=qaTables.getString(1);

			if(missingTables.contains(tableName)){
				missingColumns= new ArrayList<String>();
				//table is there in QA and Support
				//compare for columns in that table
				missingTables.remove(tableName);

				System.out.println("Comparing the Column details of Table - "+tableName);
				psmt = devConn.prepareStatement(columnQuery);
				psmt.setString(1, tableName);
				psmt.setString(2, devSchema);
				devColumns=psmt.executeQuery();

				psmt = qaConn.prepareStatement(columnQuery);
				psmt.setString(1,tableName);
				psmt.setString(2, qaSchema);
				qaColumns=psmt.executeQuery();

				while (devColumns.next()){
					devColumnDetails=new StringBuffer();
					devColumnDetails.append(devColumns.getString(1)).append(SEPARATOR).append(devColumns.getString(2)).append(SEPARATOR);
					devColumnDetails.append(devColumns.getString(3));
					missingColumns.add(devColumnDetails.toString());

				}
				while (qaColumns.next()){
					qaColumnDetails=new StringBuffer();
					qaColumnDetails.append(qaColumns.getString(1)).append(SEPARATOR).append(qaColumns.getString(2)).append(SEPARATOR);
					qaColumnDetails.append(qaColumns.getString(3));


					if(missingColumns.contains(qaColumnDetails.toString())){

						missingColumns.remove(qaColumnDetails.toString());

					}
				}
				if(missingColumns.size()!=0){
					out.write("------Mismatching columns In QA  In Table "+qaTables.getString(1)+"-----");
					out.newLine();
					out.write("COLUMN_NAME  -   DATA_TYPE   -   COLUMN_SIZE");
					out.newLine();
					out.write("__________________________________________________________");
					out.newLine();

					for(String columnName :missingColumns){
						out.write(columnName);
						out.newLine();
					}
					out.write("----------------------------------------------------");
					out.newLine();
					out.newLine();
					out.newLine();
					out.newLine();
				}



			}
		}

		if(missingTables.size()!=0){
			out.newLine();
			out.write("Missing tables in GrnHills(QA) over Dev(Support) are as follows");
			out.write("--------------------------------------------------------------");
			for (String missinTables : missingTables) {
				out.write(missinTables);
				out.newLine();
			}
			out.write("--------------------------------------------------------------");
			out.newLine();
		}
		 
		//checking the foreign key Constraints

		System.out.println("Checking for Foreign keys....");
		psmt = devConn.prepareStatement(fkQuery);
		psmt.setString(1, devSchema);
		psmt.setString(2, devSchema);
		devFkNames = psmt.executeQuery();

		psmt = qaConn.prepareStatement(fkQuery);
		psmt.setString(1, qaSchema);
		psmt.setString(2, qaSchema);
		qaFkNames = psmt.executeQuery();

		missingFkNames = new ArrayList<String>();

		while (devFkNames.next()) {
			missingFkNames.add(devFkNames.getString(1));

		}

		while (qaFkNames.next()) {
			missingFkNames.remove(qaFkNames.getString(1));
		}

		out.newLine();
		if(missingFkNames.size()!=0){
			out.write("Comparing Foreign Key ID's in GrnHills(QA) over Dev(Support) are as follows");
			out.newLine();
			for(String missingFkName:missingFkNames){
				psmt = devConn.prepareStatement(fkDetails);
				psmt.setString(1, missingFkName);
				fkDetailsSet = psmt.executeQuery();
				out.newLine();
				out.newLine();
				out.write("Missing Foreign key --"+missingFkName+" Details");
				out.newLine();
				out.newLine();
				while (fkDetailsSet.next()){
					out.write("Primary Key table Name - "+fkDetailsSet.getString(1));
					out.newLine();
					out.write("Primary Key column Name - "+fkDetailsSet.getString(2));
					out.newLine();
					out.write("Foreign Key table Name - "+fkDetailsSet.getString(3));
					out.newLine();
					out.write("Foreign Key column Name - "+fkDetailsSet.getString(4));
					out.newLine();
					out.newLine();
					out.newLine();
					out.newLine();
				}
			}

		}

	}



}
