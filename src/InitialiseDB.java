import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.sql.*;
import java.util.Scanner;

/**
* This class is used to initialise the database.
* It fufills Requirerment 3 of CS1003 P3.
* IMPORTANT: PLEASE CHECK THE "EXAMPLE" SECTION OF THE REPORT FOR INSTRUCTIONS ON HOW TO RUN THE PROGRAM
* 
* @see https://studres.cs.st-andrews.ac.uk/CS1003/Coursework/P3-DB/P3-JDBC-DBLP.pdf
* @author Antoine Megarbane, Matriculation number: 220004481
*/

public class InitialiseDB {
    public static void main(String[] args) {
        try {
            InitialiseDB engine = new InitialiseDB();
            engine.run(args[0]);
        }
        catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
    * This method initialises the database by reading the DDL.txt file, and prints out "OK" when done
    *
    * @param pathToDB The path to the database
    * @throws SQLException An SQLException
    */

    public void run(String pathToDB) throws SQLException {
        File database = new File(pathToDB);
        
        // Delete the SQLite database file if it already exists
        if (database.exists()) {
            database.delete();
        }

        // Create the SQLite database file
        try {
            database.createNewFile();
        }
        catch (IOException ex) {
            System.out.println(ex.getMessage());
        }

        Connection connection = null;
		try {
			// Connect to the Database Management System
			String dbUrl = "jdbc:sqlite:" + pathToDB;
			connection = DriverManager.getConnection(dbUrl);

            try {
                // Read the DDL.txt file line by line
                Scanner ddlReader = new Scanner(new FileReader("../DDL.txt"));
                String line;

                Statement statement = connection.createStatement();
                while (ddlReader.hasNext()) {
                    line = ddlReader.nextLine().replaceAll(";", ""); // Removing the ";" at the end of the line
                    statement.executeUpdate(line);
                }
                statement.close();
                System.out.println(checkInitial(connection));
            }
            catch (FileNotFoundException e) {
                System.out.println(e.getMessage());
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
	* Test if the four tables have been initialised
    *
	* @param connection A Connection object to connect to the database
    * @return "OK" if the database has been initialised properly, "Not OK" and the name of the table if not
	*/

    public String checkInitial(Connection connection) {
        String[] tables = new String[]{"Persons", "Venues", "Publications", "Links"};
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT name FROM sqlite_master WHERE type='table'");
            int c = 0;
            while (resultSet.next()) { 
                String name = resultSet.getString(1);
                if (!name.equals(tables[c])) {
                    statement.close();
                    System.out.println("Not OK");
                    return "Table " + tables[c] + " has not been initialised properly";
                }
                else {
                    c++;
                }
            }
            statement.close();
            return "OK";
        }
        catch (SQLException ex) {
            System.out.println(ex.getMessage());
            return "Not OK";
        }
    }
}