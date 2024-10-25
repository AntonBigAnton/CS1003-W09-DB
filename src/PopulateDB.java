import java.sql.*;

import java.nio.charset.StandardCharsets;
import java.net.URLEncoder;
import java.io.UnsupportedEncodingException;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.*;
import org.w3c.dom.*;

import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

/**
* This class is used to populate the database.
* It fufills Requirerment 4 of CS1003 P3.
* IMPORTANT: PLEASE CHECK THE "EXAMPLE" SECTION OF THE REPORT FOR INSTRUCTIONS ON HOW TO RUN THE PROGRAM
* 
* @see https://studres.cs.st-andrews.ac.uk/CS1003/Coursework/P3-DB/P3-JDBC-DBLP.pdf
* @author Antoine Megarbane, Matriculation number: 220004481
*/

public class PopulateDB {
    /**
    * A Connection object used throughout the class to connect to the database
    */
    private Connection connection;

    public static void main(String[] args) throws SQLException {
        PopulateDB queryDBLP = new PopulateDB();
        queryDBLP.connection = null;
        try {
            // Connect to the Database Management System
			String dbUrl = "jdbc:sqlite:" + args[0];
			queryDBLP.connection = DriverManager.getConnection(dbUrl);

            queryDBLP.searchAuthor("Alan Dearle");
            queryDBLP.searchAuthor("Ian Gent");
            queryDBLP.searchAuthor("Ozgur Akgun");
		}
        catch (SQLException e) {
			System.out.println(e.getMessage());
		}
        finally {
			// Regardless of whether an exception occurred above or not, 
			// make sure we close the connection to the Database Management System 
			if (queryDBLP.connection != null) queryDBLP.connection.close();
		}
    }

    /**
    * The next 4 methods are (nearly) identical to the methods in the CS1002P2 class.
    * The only difference is in the getURL method: its type is now String (previously void), as it now returns the author's url
    */

    /**
    * Get the author's url according to his/her name
    * THIS METHOD IS RE-USED FROM CS1003 P2
    * 
    * @param name The author's name
    * @return The author's url
    */

    public String getURL(String name) {
        String url = "https://dblp.org/search/author/api?format=xml&c=0&h=40&q=";

        // Step 1: replace any space character with a "+" in the author's name
        String query = name.replaceAll(" ", "+");

        // Step 2: add the query to the url
        url += query;

        return url;
    }

    /**
    * Encode the inputted url by using the URLEncoder.encode method
    * THIS METHOD IS RE-USED FROM CS1003 P2
    * 
    * @param url The author's url
    * @return The author's url but encoded
    */

    public String getEncodedURL(String url) {
        String encodedURL = "";
        try {
            encodedURL = URLEncoder.encode(url, StandardCharsets.UTF_8.toString());
        }
        catch (UnsupportedEncodingException ex) {
            System.out.println(ex.getMessage());
        }
        return encodedURL;
    }

    /**
    * Get the inputted xml file saved in the cache
    * THIS METHOD IS RE-USED FROM CS1003 P2
    * 
    * @param xml The author's xml url
    * @return A File containing the previous JDBC query if it exists, null if not
    */

    public File getSavedResponse(String xml) {
        String encodedURL = getEncodedURL(xml); // The files in the cache are encoded, so we have to encode the xml's url first
        String path = "../cache/" + encodedURL;
        File f = new File(path);

        if (f.isFile() && !f.isDirectory()) {
            return f;
        }
        else {
            return null;
        }
    }

    /**
    * Save the inputted xml in the cache, using DOM and StreamResult
    * THIS METHOD IS RE-USED FROM CS1003 P2
    * 
    * @param xml The author's xml
    */

    public void saveResponse(String xml) {
        String encodedURL = getEncodedURL(xml); // Encode the xml's url
        String path = "../cache/" + encodedURL;
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document doc = db.parse(xml);

            TransformerFactory transformerFactory =  TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            DOMSource source = new DOMSource(doc);
            StreamResult result =  new StreamResult(new File(path));
            transformer.transform(source, result);
        }
        catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }

    /**
    * Create the Document object using DOM
    *
    * @param url The author's url
    */

    public Document getDocument(String url) {
        Document doc = null;
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            File savedResponse = getSavedResponse(url); // Get the saved resonse file (or null if there isn't any saved response)
            if (savedResponse == null) {
                doc = db.parse(url);
                saveResponse(url); // Save the API call if there is no previously saved call
            }
            else {
                doc = db.parse(savedResponse);
            }
            doc.getDocumentElement().normalize();
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return doc;
    }

    /**
    * Search the author's xml file for his/her pid and name, and insert these values in the Persons table
    *
    * @param name The author's name
    */

    public void searchAuthor(String name) {
        String url = getURL(name); // Get the author's url
        try {
            Document doc = getDocument(url);

            NodeList nodeList = doc.getElementsByTagName("hit");
            for (int i=0; i<nodeList.getLength(); i++) {
                Node node = nodeList.item(i);
                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    Element e = (Element) node;
                    
                    // Get the author's name, pid, and the link to the author's publications
                    String authorName = e.getElementsByTagName("author").item(0).getTextContent();
                    String publicationUrl = e.getElementsByTagName("url").item(0).getTextContent();
                    String pid = publicationUrl.replaceAll("https://dblp.org/pid/", "");

                    // Insert the author in the DB
                    insertAuthor(pid, authorName);

                    // Search for the publications
                    publicationUrl += ".xml";
                    searchPublication(publicationUrl, pid);
                }
            }
        }
        catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }

    /**
    * Insert an author in the "Persons" table
    *
    * @param pid The author's ID
    * @param authorName The author's name
    * @throws SQLException An SQLException
    */

    public void insertAuthor(String pid, String authorName) {
        try {
            // Using JDBC, insert these values into the Persons table
            Statement statement = this.connection.createStatement();
            statement.executeUpdate("INSERT INTO Persons VALUES ('" + pid + "','" + authorName + "')");
            statement.close();
        }
        catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    /**
    * Search the publications' xml file for the "articles" and "inproceedings", and insert these values in the Publications table
    *
    * @param url The publications' url
    * @param pid The author's ID
    * @throws NullPointerException A NullPointerException if the publication has no venue
    */

    public void searchPublication(String url, String pid) {
        try {
            Document doc = getDocument(url);
            
            NodeList inproceedingsList = doc.getElementsByTagName("inproceedings"); // Get all "inproceedings" nodes
            NodeList articleList = doc.getElementsByTagName("article"); // Get all "article" nodes
            
            for (int i = 0; i<inproceedingsList.getLength(); i++) {
                Node node = inproceedingsList.item(i);
                Element e = (Element) node;
                String title = e.getElementsByTagName("title").item(0).getTextContent().replaceAll("'", "''");
                int pubyear = 0;
                try {
                    pubyear = Integer.parseInt(e.getElementsByTagName("year").item(0).getTextContent());
                }
                catch (NumberFormatException ex) {
                    System.out.println(ex.getMessage());
                }
                String pubkey = node.getAttributes().getNamedItem("key").getNodeValue();
                Statement statement = this.connection.createStatement();
                try {
                    String venueURL = "https://dblp.org/" + e.getElementsByTagName("url").item(0).getTextContent().split("\\.")[0] + ".xml";
                    String venkey = e.getElementsByTagName("url").item(0).getTextContent().split(".html")[0];

                    searchVenue(venueURL, venkey);

                    insertPublication(pubkey, pubyear, title, pid, venkey);
                }
                catch (NullPointerException ex) { // Checking if the Venue exists
                    insertPublication(pubkey, pubyear, title, pid, null);
                }
                statement.close();

                insertLink(pid, pubkey);
            }

            for (int i = 0; i<articleList.getLength(); i++) {
                Node node = articleList.item(i);
                Element e = (Element) node;
                String title = e.getElementsByTagName("title").item(0).getTextContent().replaceAll("'", "''");
                int pubyear = 0;
                try {
                    pubyear = Integer.parseInt(e.getElementsByTagName("year").item(0).getTextContent());
                }
                catch (NumberFormatException ex) {
                    System.out.println(ex.getMessage());
                }
                String pubkey = node.getAttributes().getNamedItem("key").getNodeValue();
                Statement statement = this.connection.createStatement();
                try {
                    String venueURL = "https://dblp.org/" + e.getElementsByTagName("url").item(0).getTextContent().split("\\.")[0] + ".xml";
                    String venkey = e.getElementsByTagName("url").item(0).getTextContent().split(".html")[0];

                    searchVenue(venueURL, venkey);

                    insertPublication(pubkey, pubyear, title, pid, venkey);
                }
                catch (NullPointerException ex) { // Checking if the Venue exists
                    insertPublication(pubkey, pubyear, title, pid, null);
                }
                statement.close();

                insertLink(pid, pubkey);
            }
        }
        catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }

    /**
    * Insert a Publication in the "Publications" table
    *
    * @param pubkey The publication's key
    * @param pubyear The year of publication
    * @param title The publication's title
    * @param pid The author's ID
    * @param venkey The publication's venue key
    * @throws SQLException An SQLException
    */

    public void insertPublication(String pubkey, int pubyear, String title, String pid, String venkey) {
        try {
            // Using JDBC, insert these values into the Publications table
            Statement statement = this.connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT title FROM Publications WHERE pubkey = '" + pubkey + "'");
            if (resultSet.getString(1) == null) { // Making sure that we are not inserting a duplicate
                if (venkey != null) {
                    statement.executeUpdate("INSERT INTO Publications VALUES ('" + pubkey + "'," + pubyear + ",'" + title + "','" + venkey + "')");
                }
                else {
                    statement.executeUpdate("INSERT INTO Publications VALUES ('" + pubkey + "'," + pubyear + ",'" + title + "')");
                }
            }
            statement.close();
        }
        catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    /**
    * Insert a link in the "Links" table
    *
    * @param pid The author's ID
    * @param pubkey The publication's key
    * @throws SQLException An SQLException
    */

    public void insertLink(String pid, String pubkey) {
        try {
            // Using JDBC, insert these values into the Links table
            Statement statement = this.connection.createStatement();
            statement.executeUpdate("INSERT INTO Links VALUES ('" + pid + "','" + pubkey + "')");
            statement.close();
        }
        catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    /**
    * Search the venue's xml file for the key and the title, and insert these values in the Venues table
    *
    * @param url The venue's url
    * @param pid The venue's key
    */

    public void searchVenue(String url, String venkey) {
        try {
            Document doc = getDocument(url);

            NodeList nodeList = doc.getElementsByTagName("bht"); // Get the "bht" node
            for (int i = 0; i<nodeList.getLength(); i++) {
                Node node = nodeList.item(i);
                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    Element e = (Element) node;
                    String title = e.getElementsByTagName("h1").item(0).getTextContent().replaceAll("'", "''").replaceAll("\n", " ");

                    insertVenues(venkey, title);
                }
            }
        }
        catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }

    /**
    * Insert a venue in the "Venues" table
    *
    * @param venkey The venue's key
    * @param title The venue's title
    * @throws SQLException An SQLException
    */

    public void insertVenues(String venkey, String title) {
        try {
            // Using JDBC, insert these values into the Venues table
            Statement statement = this.connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT title FROM Venues WHERE venkey = '" + venkey + "'");
            if (resultSet.getString(1) == null) { // Making sure we are not inserting a duplicate
                statement.executeUpdate("INSERT INTO Venues VALUES ('" + venkey + "','" + title + "')");
            }
            statement.close();
        }
        catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }
}