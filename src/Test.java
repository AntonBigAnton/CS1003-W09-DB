import java.sql.*;

/**
* This class is used to test my submission.
* It partially fufills Requirerment 6 of CS1003 P3.
* IMPORTANT: PLEASE CHECK THE "TESTING" SECTION OF THE REPORT FOR INSTRUCTIONS ON HOW TO RUN THE PROGRAM
* 
* @see https://studres.cs.st-andrews.ac.uk/CS1003/Coursework/P3-DB/P3-JDBC-DBLP.pdf
* @author Antoine Megarbane, Matriculation number: 220004481
*/

public class Test {
    public static void main(String[] args) throws SQLException {
        Connection connection = null;
        try {
            // Connect to the Database Management System
			String dbUrl = "jdbc:sqlite:../database.db";
			connection = DriverManager.getConnection(dbUrl);;

            Test test = new Test();
            test.testInitialiseDB(connection);
            System.out.println();
            test.testLoadAuthors(connection);
            System.out.println();
            test.testLoadPublications(connection);
            System.out.println();
            test.testLoadVenues(connection);
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
	* REQUIRED: The InitialiseDB program has been run
    *
	* @param connection A Connection object to connect to the database
	*/

    public void testInitialiseDB(Connection connection) {
        System.out.print("Test if the four tables have been initialised: ");
        InitialiseDB initialiseDB = new InitialiseDB();
        System.out.println(initialiseDB.checkInitial(connection));
    }

    /**
	* Test if the four authors have been added to the "Persons" table
	* REQUIRED: The InitialiseDB and PopulateDB programs have been run
    *
	* @param connection A Connection object to connect to the database
	*/

    public void testLoadAuthors(Connection connection) {
        System.out.print("Test if the four authors have been added to the \"Persons\" table: ");
        String[] authors = new String[]{"Alan Dearle", "Ian P. Gent", "Ian Gent", "Özgür Akgün"};
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT name FROM Persons");
            int c = 0;
            while (resultSet.next()) {
                String name = resultSet.getString(1);
                if (!name.equals(authors[c])) {
                    System.out.println("Not OK");
                    System.out.println("Author " + name + " has not been added properly");
                    break;
                }
                else if (c == 3) {
                    System.out.println("OK");
                }
                else {
                    c++;
                }
            }
            statement.close();
        }
        catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    /**
	* Test if the two publications given as example have been added to the "Publications" table
	* REQUIRED: The InitialiseDB and PopulateDB programs have been run
    *
	* @param connection A Connection object to connect to the database
	*/

    public void testLoadPublications(Connection connection) {
        System.out.print("Test if \"Investigating Binary Partition Power in Metric Query.\" and \"Bitpart: Exact metric search in high(er) dimensions.\" have been added to the \"Publications\" table: ");
        String[] titles = new String[]{"Investigating Binary Partition Power in Metric Query.", "Bitpart: Exact metric search in high(er) dimensions."};
        String[] pubkeys = new String[]{"conf/sebd/0001DV22", "journals/is/DearleC21"};
        try {
            Statement statement = connection.createStatement();
            for (int i=0; i<2; i++) {
                ResultSet resultSet = statement.executeQuery("SELECT title FROM Publications WHERE pubkey = '" + pubkeys[i] + "'");
                String name = resultSet.getString(1);
                if (!name.equals(titles[i])) {
                    System.out.println("Not OK");
                    System.out.println("Publication " + name + " has not been added properly");
                }
                else if (i == 1) {
                    System.out.println("OK");
                }
            }
        }
        catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    /**
	* Test if the two venues given as example have been added to the "Venues" table
	* REQUIRED: The InitialiseDB and PopulateDB programs have been run
    *
	* @param connection A Connection object to connect to the database
	*/

    public void testLoadVenues(Connection connection) {
        System.out.print("Test if \"30th SEBD, 2022: Tirrenia, Italy\" and \"Information Systems, Volume 95 January 2021\" have been added to the \"Venues\" table: ");
        String[] titles = new String[]{"30th SEBD 2022: Tirrenia, Italy", "Information Systems, Volume 95"};
        String[] pubkeys = new String[]{"conf/sebd/0001DV22", "journals/is/DearleC21"};
        try {
            Statement statement = connection.createStatement();
            for (int i=0; i<2; i++) {
                ResultSet resultSet = statement.executeQuery("SELECT Venues.title FROM Venues INNER JOIN Publications ON Venues.venkey = Publications.venkey WHERE Publications.pubkey = '" + pubkeys[i] + "'");
                String name = resultSet.getString(1);
                if (!name.equals(titles[i])) {
                    System.out.println("Not OK");
                    System.out.println("Venue " + name + " has not been added properly");
                }
                else if (i == 1) {
                    System.out.println("OK");
                }
            }
        }
        catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }
}