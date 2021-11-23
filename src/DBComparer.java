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

        // names of both db
        String db1_name = db1Props.getProperty("db_name");
        String db2_name = db2Props.getProperty("db_name");
        try {
            // get the metadata from both db
            DatabaseMetaData metaData1 = conn1.getMetaData();
            DatabaseMetaData metaData2 = conn2.getMetaData();
            Statement stm1 = conn1.createStatement();
            Statement stm2 = conn2.createStatement();
            // method of tables comparison
            compareTables(metaData1, metaData2, db1_name, db2_name, stm1, stm2);
            compareProcedures(db1_name, db2_name, metaData1, metaData2);
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

    private static void compareTables(DatabaseMetaData metaData1, DatabaseMetaData metaData2, 
    String db1_name, String db2_name, Statement stm1, Statement stm2) throws IOException {
            
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
                        System.out.println("--------------------------------------------------------------------");
                        System.out.println(" THE TABLES: "+db1_name+"/"+table1Name+" AND "+db2_name+"/"+table2Name+" HAVE THE SAME NAME!");
                        System.out.println(" ");
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
                                    String datatype1 = columnsdb1.getString("TYPE_NAME");
                                    String datatype2 = columnsdb2.getString("TYPE_NAME");
                                    Integer size1 = columnsdb1.getInt("COLUMN_SIZE");
                                    Integer size2 = columnsdb2.getInt("COLUMN_SIZE");
                                    if (!datatype1.equals(datatype2)) {
                                        System.out.println(" COLUMNS WITH THE SAME NAME BUT DIFFERENT TYPES:");
                                        System.out.println("  ");
                                        System.out.println(" "+db1_name+"/"+table1Name+"/"+column1Name+" TYPE: "+datatype1+"("+size1+")");
                                        System.out.println(" "+db2_name+"/"+table2Name+"/"+column2Name+" TYPE: "+datatype2+"("+size2+")");
                                        System.out.println(" ---- ");
                                        equalsDB = false;
                                    }
                                    if (datatype1.equals(datatype2) && !size1.equals(size2)) {
                                        System.out.println(" COLUMNS WITH THE SAME NAME AND TYPE, BUT DIFFERENT PRECISION:");
                                        System.out.println("  ");
                                        System.out.println(" "+db1_name+"/"+table1Name+"/"+column1Name+" TYPE: "+datatype1+"("+size1+")");
                                        System.out.println(" "+db2_name+"/"+table2Name+"/"+column2Name+" TYPE: "+datatype2+"("+size2+")");
                                        System.out.println(" ---- ");
                                        equalsDB = false;
                                    }
                                    columnDescribed = true;
                                }
                            }
                            // case of aditional column into the first table
                            if (!columnDescribed) {
                                System.out.println(" ADITIONAL COLUMN INTO TABLE "+db1_name+"/"+table1Name);
                                System.out.println(" Column name: "+columnsdb1.getString("COLUMN_NAME"));
                                System.out.println(" Column size: "+columnsdb1.getString("COLUMN_SIZE"));
                                System.out.println(" Column type: "+columnsdb1.getString("DATA_TYPE"));
                                System.out.println(" Is nullable: "+columnsdb1.getString("IS_NULLABLE"));
                                System.out.println(" Is autoincrement: "+columnsdb1.getString("IS_AUTOINCREMENT"));
                                System.out.println(" ---- ");
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
                                System.out.println(" ADITIONAL COLUMN INTO TABLE "+db2_name+"/"+table2Name);
                                System.out.println(" COLUMN NAME: "+columnsdb2.getString("COLUMN_NAME"));
                                System.out.println(" COLUMN SIZE: "+columnsdb2.getString("COLUMN_SIZE"));
                                System.out.println(" COLUMN TYPE: "+columnsdb2.getString("DATA_TYPE"));
                                System.out.println(" IS NULLABLE: "+columnsdb2.getString("IS_NULLABLE"));
                                System.out.println(" IS AUTOINCREMENT: "+columnsdb2.getString("IS_AUTOINCREMENT"));
                                System.out.println(" ---- ");
                                equalsDB = false;
                            }
                        }

                        // pkey comparation
                        ResultSet pk1 = metaData1.getPrimaryKeys(null, null, table1Name);
                        ResultSet pk2 = metaData2.getPrimaryKeys(null, null, table2Name);
                        pk1.next();
                        pk2.next();
                        String columnpk1 = pk1.getString("COLUMN_NAME");
                        String columnpk2 = pk2.getString("COLUMN_NAME");
                        if (!columnpk1.equals(columnpk2)) {
                            System.out.println(" DIFFERENCE BETWEEN PK KEYS ");
                            System.out.println(" ");
                            System.out.println(" "+db1_name+"/"+table1Name+" PK COLUMN: "+columnpk1);
                            System.out.println(" SEQUENCE NUMBER: "+pk1.getString("KEY_SEQ"));
                            System.out.println(" "+db2_name+"/"+table2Name+" PK COLUMN: "+columnpk2);
                            System.out.println(" SEQUENCE NUMBER: "+pk2.getString("KEY_SEQ"));
                            System.out.println(" ---- ");
                            equalsDB = false;
                        }
                        equalsDB = compareForeignKeys(metaData1, metaData2, table1Name, table2Name, equalsDB);
                        equalsDB = compareTriggers(table1Name, table2Name, stm1, stm2, db1_name, db2_name, equalsDB);
                    }
                }
                // case of aditional table in tablesdb1
                if (!table1Described) {
                    System.out.println(" ADITIONAL TABLE INTO DB "+db1_name);
                    System.out.println(" Catalog: " + tablesdb1.getString(1));
                    System.out.println(" Schema: " + tablesdb1.getString(2));
                    System.out.println(" Name: " + tablesdb1.getString(3));
                    System.out.println(" Type: " + tablesdb1.getString(4));
                    System.out.println(" Remarks: " + tablesdb1.getString(5));
                    System.out.println(" ---- ");
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
                    System.out.println(" ADITIONAL TABLE INTO DB "+db2_name);
                    System.out.println(" Catalog: " + tablesdb2.getString(1));
                    System.out.println(" Schema: " + tablesdb2.getString(2));
                    System.out.println(" Name: " + tablesdb2.getString(3));
                    System.out.println(" Type: " + tablesdb2.getString(4));
                    System.out.println(" Remarks: " + tablesdb2.getString(5));
                    System.out.println(" ---- ");
                    equalsDB = false;
                }
            }
            if (equalsDB) {
                System.out.println(" THE TABLES OF THE COMPARED DATA BASES ARE IDENTICAL! ");
            }
            System.out.println("--------------------------------------------------------------------");

        }
        catch(SQLException sqle) {
            sqle.printStackTrace();
            System.err.println("Error connecting: " + sqle);
        }

    }   
    
    private static Boolean compareForeignKeys(DatabaseMetaData metaData1, DatabaseMetaData metaData2, 
    String table1Name, String table2Name, Boolean qdb) throws SQLException {
        ResultSet fk1 = metaData1.getImportedKeys(null, null, table1Name);
        ResultSet fk2 = metaData2.getImportedKeys(null, null, table2Name);
        Set<String> commonFk = new HashSet<String>();
        Boolean describedFk = true;
        Boolean equalsdb = qdb;
        String fk1column = "", fk2column = "", pk1table = "", pk2table = "", pk1column = "", 
        pk2column = "", updaterule1 = "", updaterule2 = "", deleterule1 = "", deleterule2 = "";
        while(fk1.next()) {
            describedFk = false;
            fk2.beforeFirst();
            while(fk2.next() && !describedFk) {
                fk1column = fk1.getString("FKCOLUMN_NAME");
                fk2column = fk2.getString("FKCOLUMN_NAME");
                pk1table = fk1.getString("PKTABLE_NAME");
                pk2table = fk2.getString("PKTABLE_NAME");
                pk1column = fk1.getString("PKCOLUMN_NAME");
                pk2column = fk2.getString("PKCOLUMN_NAME");
                updaterule1 = fk1.getString("UPDATE_RULE");
                updaterule2 = fk2.getString("UPDATE_RULE");
                deleterule1 = fk1.getString("DELETE_RULE");
                deleterule2 = fk2.getString("DELETE_RULE");
                if (fk1column.equals(fk2column) && pk1table.equals(pk2table) && pk1column.equals(pk2column)) {
                    System.out.println(" FOREIGN KEYS FROM TABLE: "+table1Name+", HAVE THE SAME STRUCTURE ");
                    System.out.println(" ");
                    System.out.println(" "+table1Name+"/"+fk1column+" REFERENCES "+pk1table+"/"+pk1column);
                    System.out.println(" ---- ");
                    describedFk = true;
                }
                if (fk1column.equals(fk2column) && !pk1table.equals(pk2table)) {
                    System.out.println(" FOREIGN KEYS REFERENCING DIFFERENT TABLES ");
                    System.out.println(" ");
                    System.out.println(" "+table1Name+"/"+fk1column+" REFERENCES "+pk1table+"/"+pk1column);
                    System.out.println(" "+table2Name+"/"+fk2column+" REFERENCES "+pk2table+"/"+pk2column);
                    System.out.println(" ---- ");
                    commonFk.add(fk1column);
                    describedFk = true;
                    equalsdb = false;
                }
                if (fk1column.equals(fk2column) && pk1table.equals(pk2table) && !pk1column.equals(pk2column)) {
                    System.out.println(" FOREIGN KEYS REFERENCING DIFFERENT COLUMNS ");
                    System.out.println(" ");
                    System.out.println(" "+table1Name+"/"+fk1column+" REFERENCES "+pk1table+"/"+pk1column);
                    System.out.println(" "+table2Name+"/"+fk2column+" REFERENCES "+pk2table+"/"+pk2column);
                    System.out.println(" ---- ");
                    commonFk.add(fk1column);
                    describedFk = true;
                    equalsdb = false;
                }
                // same fkcolumn but different update rules
                if (fk1column.equals(fk2column) && !deleterule1.equals(deleterule2)) {
                    System.out.println(" DELETE RULES ARE DIFFERENT ");
                    System.out.println(" ");
                    System.out.println(" DELETE RULE 1: "+deleterule1+" VS "+ "DELETE RULE 2: "+deleterule2);
                    System.out.println(" ---- ");
                    commonFk.add(fk1column);
                    describedFk = true;
                    equalsdb = false;
                } 
                // same fkcolumn but different delete rules
                if (fk1column.equals(fk2column) && !updaterule1.equals(updaterule2)) {
                    System.out.println(" AND FK UPDATE RULES ARE DIFFERENT ");
                    System.out.println(" ");
                    System.out.println(" UPDATE RULE 1: "+updaterule1+" VS "+ "UPDATE RULE 2: "+updaterule2);
                    System.out.println(" ---- ");
                    commonFk.add(fk1column);
                    describedFk = true;
                    equalsdb = false;
                }
            }
            if (!describedFk && fk1column != "") {
                String sch1 = fk1.getString("FKTABLE_SCHEM");
                System.out.println(" ADITIONAL FK IN THE FIRST TABLE");
                pk1table = fk1.getString("PKTABLE_NAME");
                pk1column = fk1.getString("PKCOLUMN_NAME");
                updaterule1 = fk1.getString("UPDATE_RULE");
                deleterule1 = fk1.getString("DELETE_RULE");
                System.out.println(" "+sch1+"/"+table1Name+"/"+fk1column+" REFERENCES "+pk1table+"/"+pk1column);
                System.out.println(" UPDATE RULE: "+updaterule1);
                System.out.println(" DELETE RULE: "+deleterule1);
                System.out.println(" ---- ");
                equalsdb = false;
            }
        }
        fk2.beforeFirst();
        while(fk2.next()) {
            pk2column = fk2.getString("FKCOLUMN_NAME");
            if (!commonFk.contains(pk2column)) {
                String sch2 = fk2.getString("FKTABLE_SCHEM");
                System.out.println(" ADITIONAL FK IN THE SECOND TABLE");
                pk2table = fk2.getString("PKTABLE_NAME");
                pk2column = fk2.getString("PKCOLUMN_NAME");
                updaterule2 = fk2.getString("UPDATE_RULE");
                deleterule2 = fk2.getString("DELETE_RULE");
                System.out.println(" "+sch2+"/"+table2Name+"/"+fk2column+" REFERENCES "+pk2table+"/"+pk2column);
                System.out.println(" UPDATE RULE: "+updaterule2);
                System.out.println(" DELETE RULE: "+deleterule2);
                System.out.println(" ---- ");
                equalsdb = false;
            }
        }
        return equalsdb;

    }

    private static void getInfoProcedure(ResultSet procedure) throws SQLException {
        System.out.println("Catalog: "+procedure.getString("PROCEDURE_CAT"));
        System.out.println("Schema: "+procedure.getString("PROCEDURE_SCHEM"));
        System.out.println("Name: "+procedure.getString("PROCEDURE_NAME"));
        System.out.println("Remarks: "+procedure.getString("REMARKS"));
        System.out.println("Type: "+procedure.getShort("PROCEDURE_TYPE"));
        System.out.println(" ---- ");
    }
    
    private static void getInfoProcedureColumns(ResultSet procColumns) throws SQLException {
        System.out.println("Name: "+procColumns.getString("PROCEDURE_NAME"));
        System.out.println("Column Name: "+procColumns.getString("COLUMN_NAME"));
        System.out.println("Column Type: "+procColumns.getShort("COLUMN_TYPE"));
        System.out.println("Data Type: "+procColumns.getInt("DATA_TYPE"));
        System.out.println("Type Name: "+procColumns.getString("TYPE_NAME"));
        System.out.println("Is Nullable: "+procColumns.getShort("NULLABLE"));
        System.out.println(" ---- ");
    }
    
    private static void compareProcedures(String db1_name, String db2_name, DatabaseMetaData metaData1, DatabaseMetaData metaData2) throws IOException {
        try {
            ResultSet proceduresdb1 = metaData1.getProcedures(null, db1_name, null);
            ResultSet proceduresdb2 = metaData2.getProcedures(null, db2_name, null);
            ResultSet procColumnsdb1, procColumnsdb2;
            Boolean equalColumns = true;
            Boolean avaibleProcColumns1, avaibleProcColumns2;
            String procedure1Name, procedure2Name;
            Boolean avaibleProcedure1 = proceduresdb1.next();
            Boolean avaibleProcedure2 = proceduresdb2.next();
            // Procedures for the first database are printed
            if (avaibleProcedure1) {
                System.out.println("--------------------------------------------------------------------");
                System.out.println("Database procedures "+db1_name);
                System.out.println(" ---- ");
                proceduresdb1.previous();
                while (proceduresdb1.next())
                    getInfoProcedure(proceduresdb1);
            }
            // Procedures for the second database are printed
            if (avaibleProcedure2) {
                System.out.println("--------------------------------------------------------------------");
                System.out.println("Database procedures "+db2_name);
                System.out.println(" ---- ");
                proceduresdb2.previous();
                while (proceduresdb2.next())
                    getInfoProcedure(proceduresdb2);   
            }
    
            // It is placed before the first row
            proceduresdb1.beforeFirst();
            proceduresdb2.beforeFirst();
    
            // It checks if there are procedures with the same name
            while (proceduresdb1.next()) {
                while (proceduresdb2.next()) {
                    procedure1Name = proceduresdb1.getString("PROCEDURE_NAME");
                    procedure2Name = proceduresdb2.getString("PROCEDURE_NAME");
                    // If the name of the procedures is the same, it is verified that they have the same profile
                    if (procedure1Name.equals(procedure2Name)) {
                        System.out.println("--------------------------------------------------------------------");
                        System.out.println("There are two procedures that have the same name: "+db1_name+"/"+procedure1Name+" and "+db2_name+"/"+procedure2Name);
                        procColumnsdb1 = metaData1.getProcedureColumns(null, null, procedure1Name, null);
                        procColumnsdb2 = metaData2.getProcedureColumns(null, null, procedure2Name, null);
                        avaibleProcColumns1 = procColumnsdb1.next();
                        avaibleProcColumns2 = procColumnsdb2.next();
                        // It is verified that the procedures with the same name have the same profile
                        while ((avaibleProcColumns1 || avaibleProcColumns2) && equalColumns) {
                            if (avaibleProcColumns1 && avaibleProcColumns2) {
                                if (procColumnsdb1.getString("COLUMN_NAME").equals(procColumnsdb2.getString("COLUMN_NAME")) && 
                                    procColumnsdb1.getShort("COLUMN_TYPE") == (procColumnsdb2.getShort("COLUMN_TYPE")) &&
                                    procColumnsdb1.getInt("DATA_TYPE") == (procColumnsdb2.getInt("DATA_TYPE")) &&
                                    procColumnsdb1.getString("TYPE_NAME").equals(procColumnsdb2.getString("TYPE_NAME")) &&
                                    procColumnsdb1.getShort("NULLABLE") == (procColumnsdb2.getShort("NULLABLE")) &&
                                    proceduresdb1.getString("PROCEDURE_TYPE").equals(proceduresdb2.getString("PROCEDURE_TYPE"))) {
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
                        // If the procedures have the same profile, only one is displayed
                        if (equalColumns) {
                            System.out.println("Procedures "+db1_name+"/"+procedure1Name+" and "+db2_name+"/"+procedure2Name+" have the same profile");
                            System.out.println(" ---- ");
                            avaibleProcColumns1 = procColumnsdb1.next();
                            if (avaibleProcColumns1) {
                                System.out.println("Procedure parameters "+procedure1Name);
                                System.out.println(" ---- ");
                                procColumnsdb1.previous();
                                while (procColumnsdb1.next())
                                    getInfoProcedureColumns(procColumnsdb1);
                            }
                            else {
                                System.out.println("Procedures have no parameters");
                                System.out.println(" ---- ");
                            }
                        }
                        // If the procedures do not have the same profiles, both are displayed
                        else {
                            System.out.println("Procedures "+db1_name+"/"+procedure1Name+" and "+db2_name+"/"+procedure2Name+" do not have the same profile");
                            System.out.println(" ---- ");
                            avaibleProcColumns1 = procColumnsdb1.next();
                            if (avaibleProcColumns1) {
                                System.out.println("Procedure parameters "+db1_name+"/"+procedure1Name);
                                System.out.println(" ---- ");
                                procColumnsdb1.previous();
                                while (procColumnsdb1.next())
                                    getInfoProcedureColumns(procColumnsdb1);
                            }
                            else { 
                                System.out.println("Procedure "+db1_name+"/"+procedure1Name+" has no parameters");
                                System.out.println(" ---- ");
                            }
                            avaibleProcColumns2 = procColumnsdb2.next();
                            if (avaibleProcColumns2) {
                                System.out.println("Procedure parameters "+db2_name+"/"+procedure2Name);
                                System.out.println(" ---- ");
                                procColumnsdb2.previous();
                                while (procColumnsdb2.next())
                                    getInfoProcedureColumns(procColumnsdb2);
                            }
                            else {
                                System.out.println("Procedure "+db2_name+"/"+procedure2Name+" has no parameters");
                                System.out.println(" ---- ");
                                System.out.println("--------------------------------------------------------------------");
                            }
                        }
                    }
                }
                proceduresdb2.beforeFirst();
            }
        }         
        catch(SQLException sqle) {
            sqle.printStackTrace();
            System.err.println("Error connecting: " + sqle);
        }
    }
    
    private static void getInfoTrigger(ResultSet trigger) throws SQLException {
        System.out.println(" Name: "+trigger.getString("TRIGGER_NAME"));
        System.out.println(" Shooting moment: "+trigger.getString("ACTION_TIMING"));
        System.out.println(" ---- ");
    }
    
    private static Boolean compareTriggers(String table1Name, String table2Name, 
    Statement stm1, Statement stm2, String db1_name, String db2_name, Boolean qdb) throws IOException, SQLException {
        String query1 = "SELECT * FROM INFORMATION_SCHEMA.TRIGGERS WHERE EVENT_OBJECT_TABLE='"+table1Name+"' AND TRIGGER_SCHEMA='"+db1_name+"'";
        String query2 = "SELECT * FROM INFORMATION_SCHEMA.TRIGGERS WHERE EVENT_OBJECT_TABLE='"+table2Name+"' AND TRIGGER_SCHEMA='"+db2_name+"'";
        ResultSet triggersdb1 = stm1.executeQuery(query1);
        ResultSet triggersdb2 = stm2.executeQuery(query2);
        Set<String> commonTriggers = new HashSet<String>();
        Boolean describedTrigg = true;
        Boolean equalsdb = qdb;
        String trigg1Name, trigg2Name, trigg1ActionT, trigg2ActionT;
        while(triggersdb1.next()) {
            describedTrigg = false;
            triggersdb2.beforeFirst();
            while(triggersdb2.next() && !describedTrigg) {
                trigg1Name = triggersdb1.getString("TRIGGER_NAME");
                trigg2Name = triggersdb2.getString("TRIGGER_NAME");
                trigg1ActionT = triggersdb1.getString("ACTION_TIMING");
                trigg2ActionT = triggersdb2.getString("ACTION_TIMING");
                if (trigg1Name.equals(trigg2Name)) {
                    if (trigg1ActionT.equals(trigg2ActionT)){
                        System.out.println(" ");
                        System.out.println(" THE TABLES WITH NAME: "+table1Name+", HAVE THE FOLLOWING TRIGGER IN COMMON:");
                        System.out.println(" Trigger name: "+trigg1Name);
                        System.out.println(" Shooting moment: "+trigg1ActionT);
                        System.out.println(" ---- ");
                        describedTrigg = true;
                        commonTriggers.add(trigg1Name);
                    } else {
                        System.out.println(" ");
                        System.out.println(" COMMON TABLE: "+table1Name+" HAVE A EQUALLY NAMED TRIGGER: "+trigg1Name);
                        System.out.println(" BUT WITH DIFFERENT SHOOTING MOMENTS");
                        System.out.println(" ");
                        System.out.println(" Trigger1 shooting moment: "+trigg1ActionT);
                        System.out.println(" Trigger2 shooting moment: "+trigg2ActionT);
                        System.out.println(" ---- ");
                        describedTrigg = true;
                        commonTriggers.add(trigg1Name);
                        equalsdb = false;
                    }
                }
            }
            // aditional trigger from first table
            if (!describedTrigg) {
                System.out.println(" ");
                System.out.println(" ADITIONAL TRIGGER INTO TABLE "+db1_name+"/"+table1Name);
                getInfoTrigger(triggersdb1);
                equalsdb = false;
            }
        }
        // aditional triggers from second table
        triggersdb2.beforeFirst();
        while(triggersdb2.next()) {
            trigg2Name = triggersdb2.getString("TRIGGER_NAME");
            if (!commonTriggers.contains(trigg2Name)) {
                System.out.println(" ");
                System.out.println(" ADITIONAL TRIGGER INTO TABLE "+db2_name+"/"+table2Name);
                getInfoTrigger(triggersdb2);
                equalsdb = false;
            }
        }
        return equalsdb;
    }
}