package account;

import java.util.Map;
import java.util.List;
import java.io.FileReader;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.util.LinkedHashMap;

/**
 * Entity class for Database, consists of all the
 * basic operations to perform on Database
 */
public class Database {

    private final String name;
    private List<Table> tables;
    private final String databaseId;

    public Database(String databaseId, String name) {
        this.databaseId = databaseId;
        this.name = name;
        this.tables = new ArrayList<Table>();
    }

    /**
     * returns the name of the database instance
     * 
     * @return String
     */
    public String getDatabaseName() {
        return name;
    }

    /**
     * returns id of database instance
     * 
     * @return String
     */
    public String getDatabaseId() {
        return databaseId;
    }

    /**
     * 
     * 
     * @return List
     *         <Table>
     */
    public List<Table> getTables() {
        return tables;
    }

    /**
     * @param tableName
     * @return Table
     */
    public Table fetchTable(String tableName) {
        for (Table table : this.tables) {
            if (table.getTableName().equals(tableName)) {
                return table;
            }
        }
        return null;
    }

    /**
     * prepares database whenever the user is authenticated.
     */
    public void prepareDatabase(String fileName) {
        try {
            this.tables = prepareTables(fileName);
        } catch (Exception ex) {
            System.out.println("Error while loading database.");
        }
    }

    /**
     * parses the text file for tables and stores it in the instance
     * 
     * @param fileName
     * @return List
     *         <Table>
     * @throws Exception
     */
    public static List<Table> prepareTables(String fileName) throws Exception {
        List<Table> tables = new ArrayList<Table>();
        Table currentTable = null;
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("startTable")) {
                    String tableName = line.substring(12).trim();
                    currentTable = new Table(tableName);// add the columns and values initial value to table.
                } else if (line.startsWith("endTable")) {
                    tables.add(currentTable);
                    currentTable = null;
                } else if (currentTable != null) {
                    if (line.contains(":")) {
                        Map<String, String> columns = new LinkedHashMap<String, String>();
                        String[] columnData = line.substring(5).trim().split(", ");
                        for (String column : columnData) {
                            String[] meta = column.trim().split(" ");
                            String columnName = meta[0].trim();
                            String type = meta[1].trim();
                            columns.put(columnName, type);
                        }
                        currentTable.setColumns(columns);
                    } else {
                        String[] values = line.split(",");
                        Map<String, Object> row = new LinkedHashMap<String, Object>();
                        Object[] columNames = currentTable.getColumns().keySet().toArray();
                        for (int i = 0; i < values.length; i++) {
                            String columnName = (String) columNames[i];
                            row.put(columnName, values[i].trim());
                        }
                        currentTable.addRecord(row);
                    }
                }
            }
        } catch (Exception ex) {
            System.out.println("Error while loading the Database");
            throw new Exception("Error while loading the Database");
        }
        return tables;
    }

    /**
     * checks whether the table exists or not in database
     * 
     * @param tableName
     * @return boolean
     */
    public boolean isValidTable(String tableName) {
        for (Table table : this.tables) {
            if (table.getTableName().equals(tableName)) {
                return true;
            }
        }
        return false;
    }

}
