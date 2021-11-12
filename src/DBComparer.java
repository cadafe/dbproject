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

            // tables
            ResultSet tablesdb1 = metaData1.getTables(null, db1_name, null, tipo);
            ResultSet tablesdb2 = metaData2.getTables(null, db2_name, null, tipo);
            
            // names of the tables/columns String vars
            String table1Name, table2Name, column1Name, column2Name;
            
            // check if both tables are identical
            if (tablesdb1.equals(tablesdb2)) {
                System.out.println("Las tablas de ambas bases de datos son iguales");
            } 
            else {
                // loop over tables result sets to compare
                while(tablesdb1.next() || tablesdb2.next()) {
                    if (tablesdb1 != null && tablesdb2 != null) {
                        table1Name = tablesdb1.getString("TABLE_NAME");
                        table2Name = tablesdb2.getString("TABLE_NAME");
                        if (table1Name == table2Name) {
                            // get columns of the tables with the same name
                            ResultSet columnsdb1 = metaData1.getColumns(null, table1Name, null, null);
                            ResultSet columnsdb2 = metaData2.getColumns(null, table2Name, null, null);

                            if (columnsdb1.equals(columnsdb2)) 
                                System.out.println("Las tablas "+table1Name+" y "+table2Name+" tienen las mismas columnas");
                            else {
                                while (columnsdb1.next() || columnsdb2.next()){
                                    if (columnsdb1 != null && columnsdb2 != null) {
                                        column1Name = columnsdb1.getString("COLUMN_NAME");
                                        column2Name = columnsdb2.getString("COLUMN_NAME");
                                        String datatype1 = columnsdb1.getString("DATA_TYPE"), datatype2=columnsdb2.getString("DATA_TYPE");
                                        // check data type of columns with the same name
                                        if (column1Name == column2Name && datatype1 != datatype2) {
                                            System.out.println(" ------------------------------------------------------ ");
                                            System.out.println("DIFERENCIA DE TIPOS: "+datatype1+"EN "+column1Name+" "+ db1_name+" Y "
                                            +datatype2+"EN "+column2Name+" "+ db2_name);
                                        }
                                    }
                                    if (columnsdb1 != null && columnsdb2 == null) {
                                        System.out.println(" ------------------------------------------------------ ");
                                        System.out.println("COLUMNA ADICIONAL DE LA TABLA "+table1Name+" DE LA DB "+db1_name);
                                        System.out.println("Column name: "+columnsdb1.getString("COLUMN_NAME"));
                                        System.out.println("Column size: "+columnsdb1.getString("COLUMN_SIZE"));
                                        System.out.println("Column type: "+columnsdb1.getString("DATA_TYPE"));
                                        System.out.println("Is nullable: "+columnsdb1.getString("IS_NULLABLE"));
                                        System.out.println("Is autoincrement: "+columnsdb1.getString("IS_AUTOINCREMENT"));
                                        System.out.println(" ------------------------------------------------------ ");
                                    }
                                    if (columnsdb1 == null && columnsdb2 != null) {
                                        System.out.println(" ------------------------------------------------------ ");
                                        System.out.println("COLUMNA ADICIONAL DE LA TABLA "+table2Name+" DE LA DB "+db2_name);
                                        System.out.println("Column name: "+columnsdb2.getString("COLUMN_NAME"));
                                        System.out.println("Column size: "+columnsdb2.getString("COLUMN_SIZE"));
                                        System.out.println("Column type: "+columnsdb2.getString("DATA_TYPE"));
                                        System.out.println("Is nullable: "+columnsdb2.getString("IS_NULLABLE"));
                                        System.out.println("Is autoincrement: "+columnsdb2.getString("IS_AUTOINCREMENT"));
                                        System.out.println(" ------------------------------------------------------ ");
                                    }
                                }
                            }


                        }
                    } 
                    // aca van 2 if mas con casos similares a los if de las columnas null nonull y nonull null

            }    
        }
    
        
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