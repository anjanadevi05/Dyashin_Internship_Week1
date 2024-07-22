package com.dt.core;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseDemo {

	public static void main(String[] args) {
		
//		Load and register the driver
		try {
			
			Class.forName("com.mysql.cj.jdbc.Driver");
			
//		establish the connection between your app and db
//			url - jdbc:mysql://localhost:3306/jdbc
//			user - root
//			password - 1234
			Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/jdbc", "root", "1502");

//			Statement statement = connection.createStatement();
//			
//			int row = statement.executeUpdate("insert into student(name, percentage, address) values('Jane',70.2,'Coimbatore')");
			
			PreparedStatement preparedStatement = connection.prepareStatement("insert into student(name, percentage, address) values(?,?,?)");
			
//			preparedStatement.setString(1, "Jane Doe");
//			preparedStatement.setDouble(2, 72.12);
//			preparedStatement.setString(3, "Udhagamandalam");
//			
//			int rows = preparedStatement.executeUpdate();
//			
//			System.out.println(rows);
			
			ResultSet resultSet = preparedStatement.executeQuery("select * from student");
			
			while(resultSet.next()) {
				String name = resultSet.getString("name");
				Double percentage = resultSet.getDouble("percentage");
				String place = resultSet.getString("address");
				System.out.println(name + " ====> " + place);
			}
			
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}
}
