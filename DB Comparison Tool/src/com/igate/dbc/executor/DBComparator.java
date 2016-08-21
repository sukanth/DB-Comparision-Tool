/**
 *****************************************************************************************************************
 * File Name     : DBComparator
 * Description   : Main class to connect to  the DB's and compare  
 * Project       : DB Comparison Tool
 * Package       : com.igate.dbc.executor
 * Author        : Ravi Kishore Medarmitla
 * Date			 : Aug 17, 2015
 *
 * ****************************************************************************************************************
 */
package com.igate.dbc.executor;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;

import com.igate.dbc.service.DBComparatorService;

public class DBComparator {

	public static void main(String[] args) {
		
		
		DBComparatorService DBCService=new DBComparatorService();
		BufferedWriter out =null;
		FileWriter fstream =null;
		String fileLocation=null;

		
		try {
			fileLocation="C://DBCompareReport.txt";
			fstream = new FileWriter(fileLocation);
			out = new BufferedWriter(fstream);
			
			DBCService.connectToDb1();
			DBCService.connectToDb2();
			DBCService.compareDB(out);
			System.out.println("Comparison Completed Sucessfully....");
			System.out.println("Check the Report in this location"+fileLocation);
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		finally{
			
			try {
				out.close();
			} catch (IOException e) {
				
				e.printStackTrace();
			}
			
		}
		

	}

}
