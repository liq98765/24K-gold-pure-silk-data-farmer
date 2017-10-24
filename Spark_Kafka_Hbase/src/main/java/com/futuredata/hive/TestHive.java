package com.futuredata.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TestHive {

	public static void main(String[] args) throws SQLException {
		String url = "jdbc:hive2://ubuntu:10000/default";
		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Connection conn = DriverManager.getConnection(url, "hive", "hive");
		Statement stmt = conn.createStatement();
		String sql = "SELECT * FROM doc1 limit 10";
		System.out.println("Running" + sql);
		ResultSet res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println("id: " + res.getInt(1) + "\ttype: "
					+ res.getString(2) + "\tauthors: " + res.getString(3)
					+ "\ttitle: " + res.getString(4) + "\tyear:"
					+ res.getInt(5));
		}
	}

}