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
        
        // Initialize two connections to compare
        Connection conn1 = getConnection(db1Props); 
        Connection conn2 = getConnection(db2Props);
        try {
            // get the metadata from both db
            DatabaseMetaData metaData1 = conn1.getMetaData();
            DatabaseMetaData metaData2 = conn2.getMetaData();
            Statement stm1 = conn1.createStatement();
            Statement stm2 = conn2.createStatement();
            // method of tables comparison
            compareTables(metaData1, metaData2);
            compareProcedures(metaData1, metaData2);
            compareTriggers(stm1, stm2);
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

    private static void compareTables(DatabaseMetaData metaData1, DatabaseMetaData metaData2) throws IOException {
        Properties db1Props = getProperties(pathdb1); // Properties from "database1.properties"
        Properties db2Props = getProperties(pathdb2); // Properties from "database2.properties"
        
        // names of both db
        String db1_name = db1Props.getProperty("db_name");
        String db2_name = db2Props.getProperty("db_name");
        
        String[] tipo = {"TABLE"};
        Boolean equalsDB = true;
        try {
            // tables from db1 and db2
            ResultSet tablesdb1 = metaData1.getTables(null, db1_name, null, tipo);
            ResultSet tablesdb2 = metaData2.getTables(null, db2_name, null, tipo);
            
            // names of the tables/columns String vars
            String table1Name, table2Name, column1Name, column2Name;

            // set of visited common tables from both db
            Set<String> commonTables = new HashSet<String>();
            // flag to print aditional tables
            Boolean table1Described = true;
            while(tablesdb1.next()) {
                table1Described = false;
                // name of table from db1
                table1Name = tablesdb1.getString("TABLE_NAME");
                // loop for each table of ResultSet tablesdb1
                tablesdb2.beforeFirst(); 
                while(tablesdb2.next() && !table1Described) {
                    // name of table from db2
                    table2Name = tablesdb2.getString("TABLE_NAME");
                    // case of tables with the same name
                    if (table1Name.equals(table2Name)) {   
                        // put into common tables set
                        commonTables.add(table1Name);
                        ResultSet columnsdb1 = metaData1.getColumns(null, null, table1Name, null);
                        ResultSet columnsdb2 = metaData2.getColumns(null, null, table2Name, null);
                        System.out.println(" ------------------------------------------------------ ");
                        System.out.println(" THE TABLES: "+db1_name+"/"+table1Name+" AND "+db2_name+"/"+table2Name+" HAVE THE SAME NAME!");
                        System.out.println(" ------------------------------------------------------ ");


                        // set of visited common columns from both tables   
                        Set<String> commonColumns = new HashSet<String>();
                        // flag to print aditional columns
                        Boolean columnDescribed = true;
                        while(columnsdb1.next()) {
                            columnDescribed = false;
                            column1Name = columnsdb1.getString("COLUMN_NAME");
                            columnsdb2.beforeFirst();
                            while(columnsdb2.next() && !columnDescribed) {
                                column2Name = columnsdb2.getString("COLUMN_NAME");
                                if (column1Name.equals(column2Name)) {
                                    // put into common columns set
                                    commonColumns.add(column1Name);
                                    String datatype1 = columnsdb1.getString("DATA_TYPE");
                                    String datatype2 = columnsdb2.getString("DATA_TYPE");
                                    if (!datatype1.equals(datatype2)) {
                                        System.out.println(" ------------------------------------------------------ ");
                                        System.out.println(" COLUMNS WITH THE SAME NAME BUT DIFFERENT TYPES!");
                                        System.out.println(" "+db1_name+"/"+table1Name+"/"+column2Name+" TYPE: "+datatype1);
                                        System.out.println(" "+db2_name+"/"+table2Name+"/"+column2Name+" TYPE: "+datatype2);
                                        System.out.println(" ------------------------------------------------------ ");


                                        equalsDB = false;
                                    }
                                    columnDescribed = true;
                                }
                            }
                            // case of aditional column into the first table
                            if (!columnDescribed) {
                                System.out.println(" ------------------------------------------------------ ");
                                System.out.println(" ADITIONAL COLUMN INTO TABLE "+db1_name+"/"+table1Name);
                                System.out.println(" Column name: "+columnsdb1.getString("COLUMN_NAME"));
                                System.out.println(" Column size: "+columnsdb1.getString("COLUMN_SIZE"));
                                System.out.println(" Column type: "+columnsdb1.getString("DATA_TYPE"));
                                System.out.println(" Is nullable: "+columnsdb1.getString("IS_NULLABLE"));
                                System.out.println(" Is autoincrement: "+columnsdb1.getString("IS_AUTOINCREMENT"));
                                System.out.println(" ------------------------------------------------------ ");
                                equalsDB = false;
                            }
                        }
                        table1Described = true;
                        // case of aditional column into the second table
                        columnsdb2.beforeFirst(); // resets the cursor of columnsdb2
                        while (columnsdb2.next() && table2Name.equals(columnsdb2.getString("TABLE_NAME"))) {
                            column2Name = columnsdb2.getString("COLUMN_NAME");
                            // check if the name was visited previously, if not print as aditional column
                            if (!commonColumns.contains(column2Name)) {
                                System.out.println(" ------------------------------------------------------ ");
                                System.out.println(" ADITIONAL COLUMN INTO TABLE "+db2_name+"/"+table2Name);
                                System.out.println(" Column name: "+columnsdb2.getString("COLUMN_NAME"));
                                System.out.println(" Column size: "+columnsdb2.getString("COLUMN_SIZE"));
                                System.out.println(" Column type: "+columnsdb2.getString("DATA_TYPE"));
                                System.out.println(" Is nullable: "+columnsdb2.getString("IS_NULLABLE"));
                                System.out.println(" Is autoincrement: "+columnsdb2.getString("IS_AUTOINCREMENT"));
                                System.out.println(" ------------------------------------------------------ ");
                                equalsDB = false;
                            }
                        }

                        // key comparation
                        ResultSet pk1 = metaData1.getPrimaryKeys(null, null, table1Name);
                        ResultSet pk2 = metaData2.getPrimaryKeys(null, null, table2Name);
                        pk1.next();
                        pk2.next();
                        String columnpk1 = pk1.getString("COLUMN_NAME");
                        String columnpk2 = pk2.getString("COLUMN_NAME");
                        if (!columnpk1.equals(columnpk2)) {
                            System.out.println(" ------------------------------------------------------ ");
                            System.out.println(" DIFFERENCE BETWEEN PK KEYS ");
                            System.out.println(" "+db1_name+"/"+table1Name+" PK COLUMN: "+columnpk1);
                            System.out.println(" SEQUENCE NUMBER: "+pk1.getString("KEY_SEQ"));
                            System.out.println(" "+db2_name+"/"+table2Name+" PK COLUMN: "+columnpk2);
                            System.out.println(" SEQUENCE NUMBER: "+pk2.getString("KEY_SEQ"));
                            System.out.println(" ------------------------------------------------------ ");
                        }
                        compareForeignKeys(metaData1, metaData2, table1Name, table2Name);


                    }
                }
                // case of aditional table in tablesdb1
                if (!table1Described) {
                    System.out.println(" ------------------------------------------------------ ");
                    System.out.println(" ADITIONAL TABLE INTO DB "+db1_name);
                    System.out.println(" Catalog: " + tablesdb1.getString(1));
                    System.out.println(" Schema: " + tablesdb1.getString(2));
                    System.out.println(" Name: " + tablesdb1.getString(3));
                    System.out.println(" Type: " + tablesdb1.getString(4));
                    System.out.println(" Remarks: " + tablesdb1.getString(5));
                    System.out.println(" ------------------------------------------------------ ");
                    equalsDB = false;
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
                    System.out.println(" ADITIONAL TABLE INTO DB "+db2_name);
                    System.out.println(" Catalog: " + tablesdb2.getString(1));
                    System.out.println(" Schema: " + tablesdb2.getString(2));
                    System.out.println(" Name: " + tablesdb2.getString(3));
                    System.out.println(" Type: " + tablesdb2.getString(4));
                    System.out.println(" Remarks: " + tablesdb2.getString(5));
                    System.out.println(" ------------------------------------------------------ ");
                    equalsDB = false;
                }
            }
            if (equalsDB) {
                System.out.println(" ---------------------------------------------------- ");
                System.out.println(" THE TABLES OF THE COMPARED DATA BASES ARE IDENTICAL! ");
                System.out.println(" ---------------------------------------------------- ");

            }
        }
        catch(SQLException sqle) {
            sqle.printStackTrace();
            System.err.println("Error connecting: " + sqle);
        }

    }   
    
    private static void compareForeignKeys(DatabaseMetaData metaData1, DatabaseMetaData metaData2, String table1Name, String table2Name) throws SQLException {
        ResultSet fk1 = metaData1.getImportedKeys(null, null, table1Name);
        ResultSet fk2 = metaData2.getImportedKeys(null, null, table2Name);
        while(fk1.next()) {
            while(fk2.next()) {
                String fk1column = fk1.getString("FKCOLUMN_NAME");
                String fk2column = fk2.getString("FKCOLUMN_NAME");
                String pk1table = fk1.getString("PKTABLE_NAME");
                String pk2table = fk2.getString("PKTABLE_NAME");
                String pk1column = fk1.getString("PKCOLUMN_NAME");
                String pk2column = fk2.getString("PKCOLUMN_NAME");
                if (fk1column.equals(fk2column) && !pk1table.equals(pk2table)) {
                    System.out.println(" ---------------------------------------------------- ");
                    System.out.println(" FOREIGN KEYS REFERENCING DIFFERENT TABLES ");
                    System.out.println(" "+table1Name+fk1column+" REFERENCES: "+pk1table);
                    System.out.println(" "+table2Name+fk2column+" REFERENCES: "+pk1table);
                    System.out.println(" ---------------------------------------------------- ");
                }
                if (fk1column.equals(fk2column) && pk1table.equals(pk2table) && !pk1column.equals(pk2column)) {
                    System.out.println(" ---------------------------------------------------- ");
                    System.out.println(" FOREIGN KEYS REFERENCING DIFFERENT COLUMNS ");
                    System.out.println(" "+table1Name+fk1column+" REFERENCES: "+pk1column);
                    System.out.println(" "+table2Name+fk2column+" REFERENCES: "+pk2column);
                    System.out.println(" ---------------------------------------------------- ");
                }
            }
        }
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
        System.out.println("Name: "+procColumns.getString(3));
        System.out.println("Column Name: "+procColumns.getString(4));
        System.out.println("Column Type: "+procColumns.getShort(5));
        System.out.println("Data Type: "+procColumns.getInt(6));
        System.out.println("Type Name: "+procColumns.getString(7));
        System.out.println(" ------------------------------------------------------ ");
    }

    private static void compareProcedures(DatabaseMetaData metaData1, DatabaseMetaData metaData2) throws IOException {
        Properties db1Props = getProperties(pathdb1); // Properties from "database1.properties"
        Properties db2Props = getProperties(pathdb2); // Properties from "database2.properties"
        
        // names of both db
        String db1_name = db1Props.getProperty("db_name");
        String db2_name = db2Props.getProperty("db_name");
        
        try {
            
            ResultSet proceduresdb1 = metaData1.getProcedures(null, db1_name, null);
            ResultSet proceduresdb2 = metaData2.getProcedures(null, db2_name, null);
            ResultSet procColumnsdb1, procColumnsdb2;
            Boolean equalColumns = true;
            Boolean avaibleProcColumns1, avaibleProcColumns2;
            String procedure1Name, procedure2Name;
            Boolean avaibleProcedure1 = proceduresdb1.next();
            Boolean avaibleProcedure2 = proceduresdb2.next();
            //Imprimo los procedimientos de la primer base de datos
            if (avaibleProcedure1) {
                System.out.println("Procedimientos de la base de datos: "+db1_name);
                proceduresdb1.previous();
                while (proceduresdb1.next())
                    getInfoProcedure(proceduresdb1);
            }
            //Imprimo los procedimientos de la segunda base de datos
            if (avaibleProcedure2) {
                System.out.println("Procedimientos de la base de datos: "+db2_name);
                proceduresdb2.previous();
                while (proceduresdb2.next())
                    getInfoProcedure(proceduresdb2);
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
                        procColumnsdb1 = metaData1.getProcedureColumns(null, null, procedure1Name, null);
                        procColumnsdb2 = metaData2.getProcedureColumns(null, null, procedure2Name, null);
                        avaibleProcColumns1 = procColumnsdb1.next();
                        avaibleProcColumns2 = procColumnsdb2.next();
                        //Verifico que los procedimientos con el mismo nombre, tengan el mismo perfil
                        while ((avaibleProcColumns1 || avaibleProcColumns2) && equalColumns) {
                            if (avaibleProcColumns1 && avaibleProcColumns2) {
                                if (procColumnsdb1.getString(4).equals(procColumnsdb2.getString(4)) || 
                                    procColumnsdb1.getString(5).equals(procColumnsdb2.getString(5)) ||
                                    procColumnsdb1.getString(5).equals(procColumnsdb2.getString(6)) ||
                                    procColumnsdb1.getString(7).equals(procColumnsdb2.getString(7))) {
                                    avaibleProcColumns1 = procColumnsdb1.next();
                                    avaibleProcColumns2 = procColumnsdb2.next();
                                }
                                else {
                                    equalColumns = false;
                                }
                            }
                            else {
                                equalColumns = false;
                            }
                                
                        }
    
                        procColumnsdb1.beforeFirst();
                        procColumnsdb2.beforeFirst();
                        // Si los procedimientos tienen el mismo perfil, lo muestro
                        if (equalColumns) {
                            System.out.println("Los procedimientos "+db1_name+"/"+procedure1Name+" y "+db2_name+"/"+procedure2Name+" tienen los mismos parametros.");
                            while (procColumnsdb1.next()) {
                                System.out.println("Parametros del procedimiento "+procedure1Name);
                                getInfoProcedureColumns(procColumnsdb1);
                            }
                        }
                        // Si los procedimientos no tienen los mismos perfiles, muestro los dos.
                        else {
                            System.out.println("Los procedimientos "+db1_name+"/"+procedure1Name+" y "+db2_name+"/"+procedure2Name+" no tienen los mismos parametros.");
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
        System.out.println("Momento de disparo: "+trigger.getString("ACTION_TIMING"));
        System.out.println(" ------------------------------------------------------ ");
    }

    private static void compareTriggers(Statement stm1, Statement stm2) throws IOException {
        Properties db1Props = getProperties(pathdb1); // Properties from "database1.properties"
        Properties db2Props = getProperties(pathdb2); // Properties from "database2.properties"
        
        // names of both db
        String db1_name = db1Props.getProperty("db_name");
        String db2_name = db2Props.getProperty("db_name");
        
        try {
            String query1 = "SELECT * FROM INFORMATION_SCHEMA.TRIGGERS WHERE TRIGGER_SCHEMA='"+db1_name+"'";
            String query2 = "SELECT * FROM INFORMATION_SCHEMA.TRIGGERS WHERE TRIGGER_SCHEMA='"+db2_name+"'";
            ResultSet triggersdb1 = stm1.executeQuery(query1);
            ResultSet triggersdb2 = stm2.executeQuery(query2);
            Boolean avaibleTrigger1 = triggersdb1.next();
            Boolean avaibleTrigger2 = triggersdb2.next();
            //Imprimo los triggers de la primer base de datos
            if (avaibleTrigger1) {
                System.out.println("Triggers de la base de datos: "+db1_name);
                triggersdb1.previous();
                while (triggersdb1.next())
                    getInfoTrigger(triggersdb1);
            }
            //Imprimo los triggers de la segunda base de datos
            if (avaibleTrigger2) {
                System.out.println("Triggers de la base de datos: "+db2_name);
                triggersdb2.previous();
                while (triggersdb2.next())
                    getInfoTrigger(triggersdb2);
            }
    
            triggersdb1.beforeFirst();
            triggersdb2.beforeFirst();
    
            while (triggersdb1.next()) {
                while (triggersdb2.next()) {
                    if (triggersdb1.getString("TRIGGER_NAME").equals(triggersdb2.getString("TRIGGER_NAME")) ||
                        triggersdb1.getString("ACTION_TIMING").equals(triggersdb2.getString("ACTION_TIMING"))) {
                            System.out.println("Los triggers "+db1_name+"/"+(triggersdb1.getString("TRIGGER_NAME"))+" y "+db2_name+"/"+(triggersdb2.getString("TRIGGER_NAME"))+" tienen el mismo nombre y el mismo momento de disparo");
                    }
                }
            }
        } 
        catch(SQLException sqle) {
            sqle.printStackTrace();
            System.err.println("Error connecting: " + sqle);
        }
    }
}