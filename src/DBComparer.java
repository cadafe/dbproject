import java.sql.*;
import java.util.Properties;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;


public class DBComparer {

    public static String pathdb1 = "resources/database1.properties";
    public static String pathdb2 = "resources/database2.properties";


    public static void main(String[] args) throws IOException {
        
        Properties db1Props = getProperties(pathdb1); // Properties from "database1.properties"
        Properties db2Props = getProperties(pathdb2); // Properties from "database2.properties"
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
                // catch boolean of .next() method for control conditions
                Boolean avaibleTables1 = tablesdb1.next();
                Boolean avaibleTables2 = tablesdb2.next();

                // loop over tables result sets to compare
                while(avaibleTables1 || avaibleTables2) {
                    if (avaibleTables1 && avaibleTables2) {
                        table1Name = tablesdb1.getString("TABLE_NAME");
                        table2Name = tablesdb2.getString("TABLE_NAME");
                        if (table1Name.equals(table2Name)) {
                            System.out.println("Las tablas tienen el mismo nombre: "+table1Name);

                            // get columns of the tables with the same name
                            ResultSet columnsdb1 = metaData1.getColumns(null, table1Name, null, null);
                            ResultSet columnsdb2 = metaData2.getColumns(null, table2Name, null, null);

                            if (columnsdb1.equals(columnsdb2)) 
                                System.out.println("Las tablas "+table1Name+" y "+table2Name+" tienen las mismas columnas");
                            else {
                                // catch boolean of .next() method for control conditions
                                Boolean avaiblecolumns1 = columnsdb1.next();
                                Boolean avaiblecolumns2 = columnsdb2.next();
                                while (avaiblecolumns1 || avaiblecolumns2){
                                    if (avaiblecolumns1 && avaiblecolumns2) {
                                        column1Name = columnsdb1.getString("COLUMN_NAME");
                                        column2Name = columnsdb2.getString("COLUMN_NAME");
                                        String datatype1 = columnsdb1.getString("DATA_TYPE"), datatype2=columnsdb2.getString("DATA_TYPE");
                                        // check data type of columns with the same name
                                        Boolean sameColumnsNames = column1Name.equals(column2Name);
                                        Boolean sameColumnsTypes = datatype1.equals(datatype2);
                                        if (sameColumnsNames && !sameColumnsTypes) {
                                            System.out.println(" ------------------------------------------------------ ");
                                            System.out.println("DIFERENCIA DE TIPOS: "+datatype1+"EN "+column1Name+" "+ db1_name+" Y "
                                            +datatype2+"EN "+column2Name+" "+ db2_name);
                                        }
                                    }
                                    // table1 has more columns than table2, list aditional ones
                                    if (!avaiblecolumns2) {
                                        System.out.println(" ------------------------------------------------------ ");
                                        System.out.println("COLUMNA ADICIONAL DE LA TABLA "+table1Name+" DE LA DB "+db1_name);
                                        System.out.println("Column name: "+columnsdb1.getString("COLUMN_NAME"));
                                        System.out.println("Column size: "+columnsdb1.getString("COLUMN_SIZE"));
                                        System.out.println("Column type: "+columnsdb1.getString("DATA_TYPE"));
                                        System.out.println("Is nullable: "+columnsdb1.getString("IS_NULLABLE"));
                                        System.out.println("Is autoincrement: "+columnsdb1.getString("IS_AUTOINCREMENT"));
                                        System.out.println(" ------------------------------------------------------ ");
                                    }
                                    // table2 has more columns than table1, list aditional ones
                                    if (!avaiblecolumns1) {
                                        System.out.println(" ------------------------------------------------------ ");
                                        System.out.println("COLUMNA ADICIONAL DE LA TABLA "+table2Name+" DE LA DB "+db2_name);
                                        System.out.println("Column name: "+columnsdb2.getString("COLUMN_NAME"));
                                        System.out.println("Column size: "+columnsdb2.getString("COLUMN_SIZE"));
                                        System.out.println("Column type: "+columnsdb2.getString("DATA_TYPE"));
                                        System.out.println("Is nullable: "+columnsdb2.getString("IS_NULLABLE"));
                                        System.out.println("Is autoincrement: "+columnsdb2.getString("IS_AUTOINCREMENT"));
                                        System.out.println(" ------------------------------------------------------ ");
                                    }
                                    // moves each column cursor foward, if its possible
                                    avaiblecolumns1 = columnsdb1.next();
                                    avaiblecolumns2 = columnsdb2.next();
                                }
                            }


                        }
                    } 
                    // db1 has more tables than db2, list aditional ones
                    if (!avaibleTables2) {
                        System.out.println(" ------------------------------------------------------ ");
                        System.out.println("TABLA ADICIONAL DE LA DB "+db1_name);
                        System.out.println("Catalog: " + tablesdb1.getString(1));
                        System.out.println("Schema: " + tablesdb1.getString(2));
                        System.out.println("Name: " + tablesdb1.getString(3));
                        System.out.println("Type: " + tablesdb1.getString(4));
                        System.out.println("Remarks: " + tablesdb1.getString(5));
                        System.out.println(" ------------------------------------------------------ ");
                    }
                    // db2 has more tables than db1, list aditional ones
                    if (!avaibleTables1) {
                        System.out.println(" ------------------------------------------------------ ");
                        System.out.println("TABLA ADICIONAL DE LA DB "+db2_name);
                        System.out.println("Catalog: " + tablesdb2.getString(1));
                        System.out.println("Schema: " + tablesdb2.getString(2));
                        System.out.println("Name: " + tablesdb2.getString(3));
                        System.out.println("Type: " + tablesdb2.getString(4));
                        System.out.println("Remarks: " + tablesdb2.getString(5));
                        System.out.println(" ------------------------------------------------------ ");
                    }
                    // moves each table cursor foward, if its possible
                    avaibleTables1 = tablesdb1.next();
                    avaibleTables2 = tablesdb2.next();
                }    
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