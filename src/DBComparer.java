import java.sql.*;
import java.util.Properties;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;


public class DBComparer {
  
    public static String pathdb1 = "resources/database.properties.1";
    public static String pathdb2 = "resources/database.properties.2";


    public static void main(String[] args) throws IOException {
        
        Properties db1Props = getProperties(pathdb1); // Properties from "database.properties.1"
        Properties db2Props = getProperties(pathdb2);   // Properties from "database.properties.2"
        // Initialize two connections to compare
        Connection conn1 = getConnection(db1Props); 
        Connection conn2 = getConnection(db2Props);
        
        // names of both db
        String db1_name;
        String db2_name;
        db1_name = db1Props.getProperty("db_name");
        db2_name = db1Props.getProperty("db_name");


        try {
            String[] tipo = {"TABLE"};
            // get the metadata from both db
            DatabaseMetaData metaData1 = conn1.getMetaData();
            DatabaseMetaData metaData2 = conn2.getMetaData();

            ResultSet tablesdb1 = metaData1.getTables(null, db1_name, null, tipo);
            ResultSet tablesdb2 = metaData2.getTables(null, db2_name, null, tipo);

            System.out.println(" tablas de la base de datos ");
        // loop over tables result sets to compare
        while(tablesdb1.next() || tablesdb2.next()) {
            // check if both tables are identical
            if (tablesdb1.equals(tablesdb2)) {
                System.out.println("Las tablas de ambas bases de datos son iguales");
            } else {

            }
            
        }
        

        ResultSet dbcolumns = metaData1.getProcedureColumns(null,"procedimientos", "cursordemoAvanzado", null);
        System.out.println(" tablas de la base de datos ");
        
        catch(SQLException sqle) {
            sqle.printStackTrace();
            System.err.println("Error connecting: " + sqle);
        }


    }


    private static Connection getConnection(Properties dbProps) throws IOException {
        String driver;
        String url;
        String username;
        String password;

        driver = dbProps.getProperty("driver");
        url = dbProps.getProperty("url");
        username = dbProps.getProperty("username");
        password = dbProps.getProperty("password");

        
        try {
            // Load database driver if not already loaded.
            Class.forName(driver);
            // Establish network connection to database.
            Connection connection = DriverManager.getConnection(url, username, password);
            connection.setAutoCommit(false);
            return connection;

        } 
        catch(ClassNotFoundException cnfe) {
            System.err.println("Error loading driver: " + cnfe);
        } 
        catch(SQLException sqle) {
            sqle.printStackTrace();
            System.err.println("Error connecting: " + sqle);
        }
        return null;
    }

    private static Properties getProperties(String filePath) throws IOException {

        Properties dbProps = new Properties();

        try {
            InputStream input = new FileInputStream(filePath);
            dbProps.load(input);
            return dbProps;
        }
        catch(IOException e) {
            throw new IOException(e);
        }
    }
 
}