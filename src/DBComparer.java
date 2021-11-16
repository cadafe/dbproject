import java.sql.*;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;


public class DBComparer {

    public static String pathdb1 = "resources/database1.properties";
    public static String pathdb2 = "resources/database2.properties";

    public static void main(String[] args) throws IOException {
        Properties db1Props = getProperties(pathdb1); // Properties from "database1.properties"
        Properties db2Props = getProperties(pathdb2); // Properties from "database2.properties"
        
        // names of both db
        String db1_name = db1Props.getProperty("db_name");
        String db2_name = db2Props.getProperty("db_name");
        
        // Initialize two connections to compare
        Connection conn1 = getConnection(db1Props); 
        Connection conn2 = getConnection(db2Props);
        try {
            // get the metadata from both db
            DatabaseMetaData metaData1 = conn1.getMetaData();
            DatabaseMetaData metaData2 = conn2.getMetaData();

            // method of tables comparison
            compareTables(metaData1, metaData2);
            
            ResultSet proceduresdb1 = metaData1.getProcedures(null, db1_name, null);
            ResultSet proceduresdb2 = metaData2.getProcedures(null, db2_name, null);
            String procedure1Name, procedure2Name;

            if (proceduresdb1.equals(proceduresdb2)) {
                System.out.println("Los procedimientos de ambas bases de datos son iguales.");
            }
            else {
                Boolean avaibleProcedures1 = proceduresdb1.next();
                Boolean avaibleProcedures2 = proceduresdb2.next();

                // Imprimo todos los procedimientos
                while (avaibleProcedures1 || avaibleProcedures2) {
                    if (avaibleProcedures1 && avaibleProcedures2) {
                        getInfoProcedure(proceduresdb1);
                        getInfoProcedure(proceduresdb2);
                    }
                    if (!avaibleProcedures2) {
                        getInfoProcedure(proceduresdb1);
                    }
                    if (!avaibleProcedures1) {
                        getInfoProcedure(proceduresdb2);
                    }
                    avaibleProcedures1 = proceduresdb1.next();
                    avaibleProcedures2 = proceduresdb2.next();
                }

                // Me posiciono antes de la primera fila
                proceduresdb1.beforeFirst();
                proceduresdb2.beforeFirst();

                // Verifico si hay procedimientos con el mismo nombre
                while (proceduresdb1.next()) {
                    while (proceduresdb2.next()) {
                        procedure1Name = proceduresdb1.getString("PROCEDURE_NAME");
                        procedure2Name = proceduresdb2.getString("PROCEDURE_NAME");
                        // Si el nombre de los procedimientos son iguales, muestro los perfiles
                        if (procedure1Name.equals(procedure2Name)) {
                            System.out.println("Hay dos procedimientos que tienen el mismo nombre: "+procedure1Name);
                            ResultSet procColumnsdb1 = metaData1.getProcedureColumns(null, null, procedure1Name, null);
                            ResultSet procColumnsdb2 = metaData2.getProcedureColumns(null, null, procedure2Name, null);
                            // Si los procedimientos tienen el mismo perfil, lo muestro
                            if (procColumnsdb1.equals(procColumnsdb2)) {
                                System.out.println("Los procedimientos "+procedure1Name+" y "+procedure2Name+" tienen los mismos parametros.");
                                while (procColumnsdb1.next()) {
                                    System.out.println("Column Name: "+procColumnsdb1.getString(4));
                                    System.out.println("Column Type: "+procColumnsdb1.getShort(5));
                                    System.out.println("Data Type: "+procColumnsdb1.getInt(6));
                                    System.out.println("Type Name: "+procColumnsdb1.getString(7));
                                }
                            }
                            // Si los procedimientos no tienen los mismos perfiles, muestro los dos.
                            else {
                                Boolean avaibleProcColumns1 = procColumnsdb1.next();
                                Boolean avaibleProcColumns2 = procColumnsdb2.next();
                                System.out.println("Los procedimientos "+procedure1Name+" y "+procedure2Name+" no tienen los mismos parametros.");
                                while (avaibleProcColumns1 || avaibleProcColumns2) {
                                    if (avaibleProcColumns1 && avaibleProcColumns2) {
                                        System.out.println("Parametros del procedimiento "+procedure1Name);
                                        getInfoProcedureColumns(procColumnsdb1);
                                        System.out.println("Parametros del procedimiento "+procedure2Name);
                                        getInfoProcedureColumns(procColumnsdb2);
                                    }
                                    if (!avaibleProcColumns2) {
                                        System.out.println("Parametros del procedimiento "+procedure1Name);
                                        getInfoProcedureColumns(procColumnsdb1);
                                    }
                                    if (!avaibleProcColumns1) {
                                        System.out.println("Parametros del procedimiento "+procedure2Name);
                                        getInfoProcedureColumns(procColumnsdb2);
                                    }
                                    avaibleProcColumns1 = procColumnsdb1.next();
                                    avaibleProcColumns2 = procColumnsdb2.next();
                                }
                            }
                        }
                    }
                }
            }

            Statement stm1 = conn1.createStatement();
            Statement stm2 = conn2.createStatement();
            String query1 = "SELECT * FROM INFORMATION_SCHEMA.TRIGGERS WHERE TRIGGER_SCHEMA='db1_name'";
            String query2 = "SELECT * FROM INFORMATION_SCHEMA.TRIGGERS WHERE TRIGGER_SCHEMA='db2_name'";
            ResultSet triggersdb1 = stm1.executeQuery(query1);
            ResultSet triggersdb2 = stm2.executeQuery(query2);

            Boolean avaibleTriggers1 = triggersdb1.next(); 
            Boolean avaibleTriggers2 = triggersdb2.next();
            while (avaibleTriggers1 || avaibleTriggers2) {
                if (avaibleTriggers1 && avaibleTriggers2) {
                    System.out.println("Trigger de la base de datos "+db1_name);
                    getInfoTrigger(triggersdb1);
                    /*System.out.println("Catalogo: "+triggersdb1.getString("TRIGGER_CATALOG"));
                    System.out.println("Schema: "+triggersdb1.getString("TRIGGER_SCHEMA"));
                    System.out.println("Name: "+triggersdb1.getString("TRIGGER_NAME"));
                    System.out.println("Evento: "+triggersdb1.getString("EVENT_MANIPULATION"));
                    System.out.println("Momento de disparo: "+triggersdb1.getString("ACTION_CONDITION"));*/

                    System.out.println("Trigger de la base de datos "+db2_name);
                    getInfoTrigger(triggersdb2);
                    /*System.out.println("Catalogo: "+triggersdb2.getString("TRIGGER_CATALOG"));
                    System.out.println("Schema: "+triggersdb2.getString("TRIGGER_SCHEMA"));
                    System.out.println("Name: "+triggersdb2.getString("TRIGGER_NAME"));
                    System.out.println("Evento: "+triggersdb2.getString("EVENT_MANIPULATION"));
                    System.out.println("Momento de disparo: "+triggersdb2.getString("ACTION_CONDITION"));*/
                }
                if (!avaibleTriggers2) {
                    System.out.println("Trigger de la base de datos "+db1_name);
                    getInfoTrigger(triggersdb1);
                    /*System.out.println("Catalogo: "+triggersdb1.getString("TRIGGER_CATALOG"));
                    System.out.println("Schema: "+triggersdb1.getString("TRIGGER_SCHEMA"));
                    System.out.println("Name: "+triggersdb1.getString("TRIGGER_NAME"));
                    System.out.println("Evento: "+triggersdb1.getString("EVENT_MANIPULATION"));
                    System.out.println("Momento de disparo: "+triggersdb1.getString("ACTION_CONDITION"));*/
                }
                if (!avaibleTriggers1) {
                    System.out.println("Trigger de la base de datos "+db2_name);
                    getInfoTrigger(triggersdb2);
                    /*System.out.println("Catalogo: "+triggersdb2.getString("TRIGGER_CATALOG"));
                    System.out.println("Schema: "+triggersdb2.getString("TRIGGER_SCHEMA"));
                    System.out.println("Name: "+triggersdb2.getString("TRIGGER_NAME"));
                    System.out.println("Evento: "+triggersdb2.getString("EVENT_MANIPULATION"));
                    System.out.println("Momento de disparo: "+triggersdb2.getString("ACTION_CONDITION"));*/
                }
                avaibleTriggers1 = triggersdb1.next();
                avaibleTriggers2 = triggersdb2.next();
            }


        }
        catch(SQLException sqle) {
            sqle.printStackTrace();
            System.err.println("Error connecting: " + sqle);
        }


    }

    private static void getInfoTrigger(ResultSet trigger) throws SQLException {
        System.out.println(" ------------------------------------------------------ ");
        System.out.println("Catalogo: "+trigger.getString("TRIGGER_CATALOG"));
        System.out.println("Schema: "+trigger.getString("TRIGGER_SCHEMA"));
        System.out.println("Name: "+trigger.getString("TRIGGER_NAME"));
        System.out.println("Evento: "+trigger.getString("EVENT_MANIPULATION"));
        System.out.println("Momento de disparo: "+trigger.getString("ACTION_CONDITION"));
        System.out.println(" ------------------------------------------------------ ");
    }

    private static void getInfoProcedure(ResultSet procedure) throws SQLException {
        System.out.println(" ------------------------------------------------------ ");
        System.out.println("Catalog: "+procedure.getString(1));
        System.out.println("Schema: "+procedure.getString(2));
        System.out.println("Name: "+procedure.getString(3));
        System.out.println("Comentarios: "+procedure.getString(4));
        System.out.println("Type: "+procedure.getShort(5));
        System.out.println(" ------------------------------------------------------ ");
    }

    private static void getInfoProcedureColumns(ResultSet procColumns) throws SQLException {
        System.out.println(" ------------------------------------------------------ ");
        System.out.println("Catalogo: "+procColumns.getString(1));
        System.out.println("Schema: "+procColumns.getString(2));
        System.out.println("Name: "+procColumns.getString(3));
        System.out.println("Column Name: "+procColumns.getString(4));
        System.out.println("Column Type: "+procColumns.getShort(5));
        System.out.println("Data Type: "+procColumns.getInt(6));
        System.out.println("Type Name: "+procColumns.getString(7));
        System.out.println(" ------------------------------------------------------ ");
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

    private static void compareTables(DatabaseMetaData metaData1, DatabaseMetaData metaData2) throws IOException {
        Properties db1Props = getProperties(pathdb1); // Properties from "database1.properties"
        Properties db2Props = getProperties(pathdb2); // Properties from "database2.properties"
        
        // names of both db
        String db1_name = db1Props.getProperty("db_name");
        String db2_name = db2Props.getProperty("db_name");
        
        String[] tipo = {"TABLE"};

        try {
            // tables from db1 and db2
            ResultSet tablesdb1 = metaData1.getTables(null, db1_name, null, tipo);
            ResultSet tablesdb2 = metaData2.getTables(null, db2_name, null, tipo);
            
            // names of the tables/columns String vars
            String table1Name, table2Name, column1Name, column2Name;

            // set of visited common tables from both db
            Set<String> commonTables = new HashSet<String>();
            
            while(tablesdb1.next()) {
                // flag to print aditional tables
                Boolean describedTable = false;
                // name of table from db1
                table1Name = tablesdb1.getString("TABLE_NAME");
                // loop for each table of ResultSet tablesdb1
                while(tablesdb2.next()) {
                    // name of table from db2
                    table2Name = tablesdb2.getString("TABLE_NAME");
                    // case of tables with the same name
                    if (table1Name.equals(table2Name)) {   
                        // put into common tables set
                        commonTables.add(table1Name);
                        ResultSet columnsdb1 = metaData1.getColumns(null, table1Name, null, null);
                        ResultSet columnsdb2 = metaData2.getColumns(null, table2Name, null, null);
                        describedTable = true;

                        // set of visited common columns from both tables   
                        Set<String> commonColumns = new HashSet<String>();
                        while(columnsdb1.next()) {
                            Boolean describedColumn = false;
                            column1Name = columnsdb1.getString("COLUMN_NAME");
                            while(columnsdb2.next()) {
                                column2Name = columnsdb2.getString("COLUMN_NAME");
                                if (column1Name.equals(column2Name)) {
                                    // put into common columns set
                                    commonColumns.add(column1Name);
                                    String datatype1 = columnsdb1.getString("DATA_TYPE");
                                    String datatype2 = columnsdb2.getString("DATA_TYPE");
                                    if (!datatype1.equals(datatype2)) {
                                        System.out.println(" ------------------------------------------------------ ");
                                        System.out.println("DIFERENCIA DE TIPOS: "+db1_name+"->"+table1Name+"->"+column2Name+" tipo: "+datatype1
                                        +" VS "+db2_name+"->"+table2Name+"->"+column2Name+" tipo: "+datatype2);
                                    }
                                    describedColumn = true;
                                }
                            }
                            // case of aditional column into the first table
                            if (!describedColumn) {
                                System.out.println(" ------------------------------------------------------ ");
                                System.out.println("COLUMNA ADICIONAL DE LA TABLA "+table1Name+" DE LA DB "+db1_name);
                                System.out.println("Column name: "+columnsdb1.getString("COLUMN_NAME"));
                                System.out.println("Column size: "+columnsdb1.getString("COLUMN_SIZE"));
                                System.out.println("Column type: "+columnsdb1.getString("DATA_TYPE"));
                                System.out.println("Is nullable: "+columnsdb1.getString("IS_NULLABLE"));
                                System.out.println("Is autoincrement: "+columnsdb1.getString("IS_AUTOINCREMENT"));
                                System.out.println(" ------------------------------------------------------ ");
                            }
                        }
                        // case of aditional column into the second table
                        columnsdb2.beforeFirst(); // resets the cursor of columnsdb2
                        while (columnsdb2.next()) {
                            column2Name = columnsdb2.getString("COLUMN_NAME");
                            // check if the name was visited previously, if not print as aditional column
                            if (!commonColumns.contains(column2Name)) {
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
                // case of aditional table in tablesdb1
                if (!describedTable) {
                    System.out.println(" ------------------------------------------------------ ");
                    System.out.println("TABLA ADICIONAL DE LA DB "+db1_name);
                    System.out.println("Catalog: " + tablesdb1.getString(1));
                    System.out.println("Schema: " + tablesdb1.getString(2));
                    System.out.println("Name: " + tablesdb1.getString(3));
                    System.out.println("Type: " + tablesdb1.getString(4));
                    System.out.println("Remarks: " + tablesdb1.getString(5));
                    System.out.println(" ------------------------------------------------------ ");
                }
            }

            // case of aditional table in tablesdb2
            // reset the cursor of the tables from db2
            tablesdb2.beforeFirst();
            while (tablesdb2.next()) {
                table2Name = tablesdb2.getString("TABLE_NAME");
                // check if the name was visited previously, if not print as aditional table
                if (!commonTables.contains(table2Name)) {
                    System.out.println(" ------------------------------------------------------ ");
                    System.out.println("TABLA ADICIONAL DE LA DB "+db2_name);
                    System.out.println("Catalog: " + tablesdb2.getString(1));
                    System.out.println("Schema: " + tablesdb2.getString(2));
                    System.out.println("Name: " + tablesdb2.getString(3));
                    System.out.println("Type: " + tablesdb2.getString(4));
                    System.out.println("Remarks: " + tablesdb2.getString(5));
                    System.out.println(" ------------------------------------------------------ ");
                }
            }
        }
        catch(SQLException sqle) {
            sqle.printStackTrace();
            System.err.println("Error connecting: " + sqle);
        }
    }   
}