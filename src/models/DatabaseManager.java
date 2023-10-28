package models;

import account.Database;
import account.Authentication;

import java.io.*;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

public class DatabaseManager {

    private static final String FILE_PATH = "database.txt";
    private static final String DATABASE_SEPARATOR = "|";

    /**
     * Fetches the list of databases from the file.
     * 
     * @return List<Database>
     */
    public static List<Database> fetchDatabase() {
        File file = new File(FILE_PATH);
        // Returns an empty list if the file does not exist.
        if (!file.exists()) {
            return new ArrayList<>();
        }

        List<Database> dbs = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(FILE_PATH))) {
            String line;
            // Read a database from the reader.
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\|");

                // Add a new Database to the list of databases.
                if (parts.length == 2) {
                    Database db = new Database(parts[0], parts[1]);
                    dbs.add(db);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return dbs;
    }

    /**
     * Saves the database to disk.
     * 
     * @param db - The database to save to disk. It must not be null
     */
    public static void saveDatabase(Database db) throws IOException {
        try (PrintWriter writer = new PrintWriter(new FileWriter(FILE_PATH, true))) {
            writer.println(db.getDatabaseId() + DATABASE_SEPARATOR + db.getDatabaseName());
        }
    }

    /**
     * Reads the database from file. If there is no database null is returned.
     * 
     * @return Database or null if not found in the file or an error occurred during
     *         reading the database information from the
     */
    public static Database getDatabaseFromFile() throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(FILE_PATH));
        String line = reader.readLine();
        reader.close();
        String[] parts = line.split("\\|");
        // Returns the Database object for the first part of the list of parts.
        if (parts.length == 2) {
            Database db = new Database(parts[0], parts[1]);
            return db;
        }
        return null;
    }

    /**
     * Evaluates a condition in the form column1 = value1 column2 = value2. This is
     * used to determine if there is a row that matches the condition
     * 
     * @param row          - the row to be evaluated
     * @param tableColumns - the columns in the table that match the condition
     * @param whereClause  - the where clause for the query ( " WHERE " )
     * 
     * @return true if the row matches the condition false otherwise ( no match or
     *         no row found in the where clause
     */
    private static boolean evaluateCondition(String row, String[] tableColumns, String whereClause) {
        String[] values = row.split(",");
        String[] condition = whereClause.split("=");
        String conditionCol = condition[0].trim();
        String conditionVal = condition[1].trim();
        // Returns true if the conditionCol and value are equal to the conditionCol and
        // conditionVal.
        for (int i = 0; i < tableColumns.length; i++) {
            // Check if the condition column and value are equal.
            if (tableColumns[i].trim().equals(conditionCol) && values[i].trim().equals(conditionVal)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Updates a record in a table. This method is used to update an existing record
     * in the database.
     * 
     * @param auth         - The authentication object used to authenticate the
     *                     request.
     * @param tableName    - The name of the table to update. This must be a table
     *                     in the database.
     * @param tableColumns - The columns that should be updated in the table.
     * @param whereClause  - The where clause for the update. If null or empty all
     *                     columns will be updated.
     * @param setClause    - The set clause to be used for the
     */
    public static void updateRecordInTable(Authentication auth, String tableName, String[] tableColumns,
            String whereClause, Map<String, String> setClause) throws Exception {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(auth.DB_FILE_PATH));
            StringBuilder databaseContent = new StringBuilder();
            String line;
            // Read a line from the reader and append it to the databaseContent.
            while ((line = reader.readLine()) != null) {
                databaseContent.append(line).append('\n');
            }
            reader.close();
            int tableStartIndex = databaseContent.indexOf("startTable - " + tableName);
            // Check if the table is found.
            if (tableStartIndex == -1) {
                System.out.println("Table not found: " + tableName);
                throw new Exception("Table not found: " + tableName);
            }
            int tableEndIndex = databaseContent.indexOf("endTable - " + tableName, tableStartIndex);
            // Check if the table has conflicts.
            if (tableEndIndex == -1) {
                System.out.println("Database error:" + tableName + " has conflicts");
                throw new Exception("Database error:" + tableName + " has conflicts");
            }
            String tableContent = databaseContent.substring(tableStartIndex, tableEndIndex);
            String[] rows = tableContent.split("\n");
            List<String> updatedRows = new ArrayList<>();
            int count = 0;
            for (String row : rows) {
                count++;
                // Evaluate the condition of a row.
                if (count >= 3 && evaluateCondition(row, tableColumns, whereClause)) {
                    String eachRow = "";
                    String[] values = row.split(",");
                    // Generates a comma separated list of column names and values.
                    for (int i = 0; i < values.length; i++) {
                        String colName = tableColumns[i].trim();
                        String colVal = values[i].trim();
                        // concatenate the column name to the row
                        if (setClause.containsKey(colName)) {
                            eachRow = eachRow.concat(setClause.get(colName).trim() + ",");
                        } else {
                            eachRow = eachRow.concat(colVal + ",");
                        }
                    }
                    eachRow = eachRow.substring(0, eachRow.length() - 1);// removing the last ,
                    updatedRows.add(eachRow);
                    continue;
                }
                updatedRows.add(row);
            }
            StringBuilder updatedTableContent = new StringBuilder();
            for (String row : updatedRows) {
                updatedTableContent.append(row).append('\n');
            }
            databaseContent.replace(tableStartIndex, tableEndIndex, updatedTableContent.toString());
            BufferedWriter writer = new BufferedWriter(new FileWriter(auth.DB_FILE_PATH));
            writer.write(databaseContent.toString());
            writer.close();
            System.out.println("Row(s) updated successfully.");
        } catch (Exception ex) {
            throw new Exception("Error while updating rows");
        }
    }

    /**
     * Deletes records from a table. This method is used to delete records from a
     * table that match the where clause and column names
     * 
     * @param auth         - The authentication object for the user
     * @param tableName    - The name of the table
     * @param tableColumns - The columns that should be deleted from the table
     * @param whereClause  - The where clause to be used in the
     */
    public static void deleteRecordInTable(Authentication auth, String tableName, String[] tableColumns,
            String whereClause) throws Exception {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(auth.DB_FILE_PATH));
            StringBuilder databaseContext = new StringBuilder();
            String line;
            // Read a line from the reader and append it to the database context.
            while ((line = reader.readLine()) != null) {
                databaseContext.append(line).append("\n");
            }
            reader.close();
            int tableStartIndex = databaseContext.indexOf("startTable - " + tableName);
            // Check if the table is found.
            if (tableStartIndex == -1) {
                throw new Exception("Table not found: " + tableName);
            }
            int tableEndIndex = databaseContext.indexOf("endTable - " + tableName, tableStartIndex);
            // Check if the table has conflicts.
            if (tableEndIndex == -1) {
                throw new Exception("Database error:" + tableName + " is improperly configured");
            }
            String tableContent = databaseContext.substring(tableStartIndex, tableEndIndex);
            String[] rows = tableContent.split("\n");
            List<String> updatedRows = new ArrayList<>();
            int count = 0;
            for (String row : rows) {
                count++;
                // If count 3 and count 3 evaluate the condition.
                if (count >= 3 && evaluateCondition(row, tableColumns, whereClause)) {
                    continue;
                }
                updatedRows.add(row);
            }
            StringBuilder updatedTableContent = new StringBuilder();
            for (String row : updatedRows) {
                updatedTableContent.append(row).append('\n');
            }
            databaseContext.replace(tableStartIndex, tableEndIndex, updatedTableContent.toString());
            BufferedWriter writer = new BufferedWriter(new FileWriter(auth.DB_FILE_PATH));
            writer.write(databaseContext.toString());
            writer.close();
            System.out.println("Rows deleted successfully.");
        } catch (Exception ex) {
            throw new Exception("Error while deleting rows");
        }
    }

    public static void copyFile(String file1, String file2, Boolean shouldDeleteSourceFile) throws Exception {
        File fromFile = new File(file1);
        File toFile = new File(file2);
        StringBuilder content = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(fromFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append("\n");
            }
        } catch (Exception ex) {
            throw new Exception("Error reading copy file.");
        }
        try (FileWriter writer = new FileWriter(toFile)) {
            writer.write(content.toString());
        } catch (IOException e) {
            throw new Exception("Error writing to original file.");
        }
        if (shouldDeleteSourceFile) {
            fromFile.delete();
        }
    }

    public static void lockDatabase() {}
}
