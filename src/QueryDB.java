import java.sql.*;

/**
* This class is used to query the database.
* It fufills Requirerment 5 of CS1003 P3.
* IMPORTANT: PLEASE CHECK THE "EXAMPLE" SECTION OF THE REPORT FOR INSTRUCTIONS ON HOW TO RUN THE PROGRAM
* 
* @see https://studres.cs.st-andrews.ac.uk/CS1003/Coursework/P3-DB/P3-JDBC-DBLP.pdf
* @author Antoine Megarbane, Matriculation number: 220004481
*/

public class QueryDB {
    public static void main(String[] args) throws SQLException {
		Connection connection = null;
        try {
			// Connect to the Database Management System
			String dbUrl = "jdbc:sqlite:../database.db";
			connection = DriverManager.getConnection(dbUrl);

			QueryDB queryDB = new QueryDB();
			int query = 0;
			try {
				query = Integer.parseInt(args[0]);
			}
			catch (NumberFormatException ex) {
				System.out.println(ex.getMessage());
			}
			// Get the query number
			switch (query) {
				case 1:
					queryDB.Query1(connection);
					break;
				case 2:
					queryDB.Query2(connection);
					break;
				case 3:
					queryDB.Query3(connection);
					break;
				case 4:
					queryDB.Query4(connection);
					break;
				case 5:
					queryDB.Query5(connection);
					break;
				default:
					System.out.println("Invalid command line argument!");
					System.out.println("Use the query number (1 to 5) as unique command line argument!");
			}
		}
		catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		finally {
			// Regardless of whether an exception occurred above or not, 
			// make sure we close the connection to the Database Management System 
			if (connection != null) connection.close();
		}
    }

	/**
	* Count the total number of publications that have been published by either "Özgür Akgün" or "Ian Gent" or "Alan Dearle"
	* This is Query 1 from CS1003 P3 R5
	*
	* @param connection A Connection object to connect to the database
	*/

    public void Query1(Connection connection) {
		try {            
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM Publications");

			System.out.print("Total number of publications that have been published by either \"Özgür Akgün\" or \"Ian Gent\" or \"Alan Dearle\": ");
			System.out.println(resultSet.getInt(1));
			
			statement.close();
		}
		catch (SQLException e) {
			System.out.println(e.getMessage());
		}
    }

	/**
	* Count the number of publications that have been published by "Özgür Akgün"
	* This is Query 2 from CS1003 P3 R5
	*
	* @param connection A Connection object to connect to the database
	*/

	public void Query2(Connection connection) {
		try {
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM Publications INNER JOIN Links ON Publications.pubkey = Links.pubkey INNER JOIN Persons ON Links.pid = Persons.pid WHERE Persons.name LIKE '_zg_r Akg_n'");

			System.out.print("Total number of publications that have been published by \"Özgür Akgün\": ");
			System.out.println(resultSet.getInt(1));

			statement.close();
		}
		catch (SQLException e) {
			System.out.println(e.getMessage());
		}
    }

	/**
	* List the titles of the publications of "Özgür Akgün"
	* This is Query 3 from CS1003 P3 R5
	*
	* @param connection A Connection object to connect to the database
	*/

	public void Query3(Connection connection) {
		try {
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery("SELECT Publications.title FROM Publications INNER JOIN Links ON Publications.pubkey = Links.pubkey INNER JOIN Persons ON Links.pid = Persons.pid WHERE Persons.name LIKE '_zg_r Akg_n'");

			System.out.println("Titles of the publications of \"Özgür Akgün\": ");
			while (resultSet.next()) {
				String title = resultSet.getString(1);
				System.out.println(title);
			}

			statement.close();
		}
		catch (SQLException e) {
			System.out.println(e.getMessage());
		}
    }

	/**
	* List the publication venues in which "Özgür Akgün" has published in the last 3 years
	* This is Query 4 from CS1003 P3 R5
	*
	* @param connection A Connection object to connect to the database
	*/

	public void Query4(Connection connection) {
		try {
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery("SELECT DISTINCT Venues.title FROM Venues INNER JOIN Publications ON Publications.venkey = Venues.venkey INNER JOIN Links ON Publications.pubkey = Links.pubkey INNER JOIN Persons ON Links.pid = Persons.pid WHERE Persons.name LIKE '_zg_r Akg_n' AND Publications.pubyear > 2020");

			System.out.println("Publication venues in which \"Özgür Akgün\" has published in the last 3 years: ");
			while (resultSet.next()) {
				String venue = resultSet.getString(1);
				System.out.println(venue);
			}

			statement.close();
		}
		catch (SQLException e) {
			System.out.println(e.getMessage());
		}
    }

	/**
	* List the names of papers, and their venues that have been authored by "Özgür Akgün"
	* This is Query 5 from CS1003 P3 R5
	*
	* @param connection A Connection object to connect to the database
	*/

	public void Query5(Connection connection) {
		try {
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery("SELECT Publications.title, Venues.title FROM Venues INNER JOIN Publications ON Publications.venkey = Venues.venkey INNER JOIN Links ON Publications.pubkey = Links.pubkey INNER JOIN Persons ON Links.pid = Persons.pid WHERE Persons.name LIKE '_zg_r Akg_n'");

			System.out.println("Names of papers, and their venues that have been authored by \"Özgür Akgün\": ");
			while (resultSet.next()) {
				String title = resultSet.getString(1);
				String venue = resultSet.getString(2);
				System.out.println("Name of paper: " + title);
				System.out.println("Venue: " + venue);
			}

			statement.close();
		}
		catch (SQLException e) {
			System.out.println(e.getMessage());
		}
    }
}