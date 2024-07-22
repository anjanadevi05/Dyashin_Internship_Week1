package com.jdbc.student;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

public class StudentDatabase {

	private static Connection connection;
	private static Scanner scanner = new Scanner(System.in);

	public static void main(String[] args) {

		StudentDatabase studentDatabase = new StudentDatabase();
		try {

			Class.forName("com.mysql.cj.jdbc.Driver");

			String url = "jdbc:mysql://localhost:3306/jdbc";
			String user = "root";
			String password = "1234";

			connection = DriverManager.getConnection(url, user, password);

			System.out.println("Press 1 for Insert record");
			System.out.println("Press 2 for Select record");
			System.out.println("Press 3 for Select all records (CallableStatement) ");
			System.out.println("Press 4 for Select record by roll number (CallableStatement)");
			System.out.println("Press 5 for Update record");
			System.out.println("Press 6 for Delete record");
			System.out.println("Press 7 for understanding the transactions");
			System.out.println("Press 8 for Batch processing");

			System.out.println("Enter Choice : ");

			int choice = Integer.parseInt(scanner.nextLine());

			switch (choice) {
			case 1:
				studentDatabase.insertRecord();
				break;
			case 2:
				studentDatabase.selectRecord();
				break;
			case 3:
				studentDatabase.selectAllRecords();
				break;
			case 4:
				studentDatabase.selectRecordByRollNumber();
				break;
			case 5:
				studentDatabase.updateRecord();
				break;
			case 6:
				studentDatabase.deleteRecord();
				break;
			case 7:
				studentDatabase.transaction();
				break;
			case 8:
				studentDatabase.batchProcessing();
				break;
			default:

				break;
			}

		} catch (ClassNotFoundException | SQLException e) {

			e.printStackTrace();
		}

	}

	private void insertRecord() throws SQLException {

		String query = "insert into student(name, percentage, address) values(?,?,?)";

		PreparedStatement preparedStatement = connection.prepareStatement(query);

		System.out.println("Enter name : ");
		preparedStatement.setString(1, scanner.nextLine());

		System.out.println("Enter percentage : ");
		preparedStatement.setDouble(2, Double.parseDouble(scanner.nextLine()));

		System.out.println("Enter address : ");
		preparedStatement.setString(3, scanner.nextLine());

		int rows = preparedStatement.executeUpdate();

		if (rows != 0)
			System.out.println("record inserted successfully !!!!");
	}

	private void selectRecord() throws SQLException {

		System.out.println("Enter roll number to find the result : ");
		int number = Integer.parseInt(scanner.nextLine());
		String query = "select * from student where roll_number = " + number;

		Statement statement = connection.createStatement();

		ResultSet resultSet = statement.executeQuery(query);

		if (resultSet.next()) {
			int rollNumber = resultSet.getInt("roll_number");
			String name = resultSet.getString("name");
			double percentage = resultSet.getDouble(3);
			String address = resultSet.getString(4);

			System.out.println("Roll number : " + rollNumber);
			System.out.println("Name : " + name);
			System.out.println("Percentage : " + percentage);
			System.out.println("Address : " + address);

		} else {
			System.out.println("No record found !");
		}

	}

	private void selectAllRecords() throws SQLException {

		CallableStatement callableStatement = connection.prepareCall("{call GET_ALL()}");

		ResultSet resultSet = callableStatement.executeQuery();

		while (resultSet.next()) {

			int rollNumber = resultSet.getInt("roll_number");
			String name = resultSet.getString("name");
			double percentage = resultSet.getDouble(3);
			String address = resultSet.getString(4);

			System.out.println("Roll number : " + rollNumber);
			System.out.println("Name : " + name);
			System.out.println("Percentage : " + percentage);
			System.out.println("Address : " + address);
			System.out.println("============================");
		}
	}

	private void selectRecordByRollNumber() throws SQLException {

		System.out.println("Enter roll number : ");
		int roll = Integer.parseInt(scanner.nextLine());

		CallableStatement callableStatement = connection.prepareCall("{call GET_RECORD(?)}");

		callableStatement.setInt(1, roll);

		ResultSet resultSet = callableStatement.executeQuery();

		while (resultSet.next()) {

			int rollNumber = resultSet.getInt("roll_number");
			String name = resultSet.getString("name");
			double percentage = resultSet.getDouble(3);
			String address = resultSet.getString(4);

			System.out.println("Roll number : " + rollNumber);
			System.out.println("Name : " + name);
			System.out.println("Percentage : " + percentage);
			System.out.println("Address : " + address);
			System.out.println("============================");
		}
	}

	private void updateRecord() throws SQLException {

		System.out.println("Enter roll number to update record : 	");
		int roll = Integer.parseInt(scanner.nextLine());
		String query = "select * from student where roll_number = " + roll;

		Statement statement = connection.createStatement();

		ResultSet resultSet = statement.executeQuery(query);

		if (resultSet.next()) {
			int rollNumber = resultSet.getInt(1);
			String name = resultSet.getString(2);
			double percentage = resultSet.getDouble(3);
			String address = resultSet.getString(4);

			System.out.println("Roll number : " + rollNumber);
			System.out.println("Name : " + name);
			System.out.println("Percentage : " + percentage);
			System.out.println("Address : " + address);

			System.out.println("Press 1 for Update Name");
			System.out.println("Press 2 for Update Percentage");
			System.out.println("Press 3 for Update Address");

			int choice = Integer.parseInt(scanner.nextLine());

			String updateQuery = "update student set ";

			switch (choice) {
			case 1:
				System.out.println("Enter new name : ");
				String newName = scanner.nextLine();

				updateQuery = updateQuery + "name = ? where roll_number = " + rollNumber;
				PreparedStatement preparedStatement = connection.prepareStatement(updateQuery);
				preparedStatement.setString(1, newName);

				int row = preparedStatement.executeUpdate();
				if (row != 0)
					System.out.println("Record Updated successfully.");
				break;
			case 2:
				System.out.println("Enter new percentage : ");
				double newPercentage = Double.parseDouble(scanner.nextLine());

				updateQuery = updateQuery + "percentage = ? where roll_number = " + rollNumber;

				PreparedStatement preparedStatement2 = connection.prepareStatement(updateQuery);
				preparedStatement2.setDouble(1, newPercentage);

				int row2 = preparedStatement2.executeUpdate();
				if (row2 != 0)
					System.out.println("Record Updated successfully.");
				break;

			case 3:
				System.out.println("Enter new address : ");
				String newAddress = scanner.nextLine();

				updateQuery = updateQuery + "address = ? where roll_number = " + rollNumber;

				PreparedStatement preparedStatement3 = connection.prepareStatement(updateQuery);
				preparedStatement3.setString(1, newAddress);

				int row3 = preparedStatement3.executeUpdate();
				if (row3 != 0)
					System.out.println("Record Updated successfully.");
				break;
			default:
				break;
			}
		} else {
			System.out.println("Record not found");
		}

	}

	private void deleteRecord() throws SQLException {

		System.out.println("Enter roll number to delete : ");
		int rollNumber = Integer.parseInt(scanner.nextLine());

		String sql = "delete from student where roll_number = " + rollNumber;

		Statement statement = connection.createStatement();

		int row = statement.executeUpdate(sql);

		if (row != 0)
			System.out.println("Record deleted successfully.");

	}

	private void transaction() throws SQLException {

		connection.setAutoCommit(false);

		String sql = "insert into student (name,percentage,address) values ('john',87.2,'kochi')";
		String sql1 = "insert into student (name,percentage,address) values ('oliver',82.2,'bangalore')";

		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		int row1 = preparedStatement.executeUpdate();

		PreparedStatement preparedStatement2 = connection.prepareStatement(sql1);
		int row2 = preparedStatement2.executeUpdate();

		if (row1 != 0 && row2 != 0)
			connection.commit();
		else
			connection.rollback();

	}

	private void batchProcessing() throws SQLException {

		connection.setAutoCommit(false);

		String sql = "insert into student (name,percentage,address) values ('john',87.2,'kochi')";
		String sql1 = "insert into student (name,percentage,address) values ('oliver',82.2,'bangalore')";
		String sql2 = "insert into student (name,percentage,address) values ('rohan',82.2,'bangalore')";

		Statement statement = connection.createStatement();

		statement.addBatch(sql);
		statement.addBatch(sql1);
		statement.addBatch(sql2);

		int[] rows = statement.executeBatch();

		for (int i : rows) {
			if (i != 0)
				continue;
			else
				connection.rollback();
		}
		connection.commit();

	}
}
