package utils;

import account.Table;
import account.Database;
import account.Authentication;
import models.DatabaseManager;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Query {

    Authentication auth;
    private static String query = "";
    private static final List<Database> dbs = DatabaseManager.fetchDatabase();

    public Query(List<String> queryInput, Authentication auth) throws Exception {
        this.auth = auth;
        sanitizeQuery(queryInput);
        parseQuery();
    }

    /**
     * @param queryInput
     */
    public static void sanitizeQuery(List<String> queryInput) {
        query = queryInput.stream().map(String::valueOf).collect(Collectors.joining(" ")).replace(";", "");
    }

    /**
     * @throws Exception
     */
    public void parseQuery() throws Exception {
        String[] queryParts = query.split(" ");
        String operation = queryParts[0].toUpperCase();

        switch (operation) {
            case "CREATE" -> parseCreate(query);
            case "SELECT" -> parseSelect(query);
            case "UPDATE" -> parseUpdate(query);
            case "INSERT" -> parseInsert(query);
            case "DELETE" -> parseDelete(query);
            case "BEGIN" -> parseBeginTransaction(query);
            case "COMMIT" -> parseCommit(query);
            case "ROLLBACK" -> parseRollback(query);
            case "END" -> parseEndTransaction(query);
            default -> throw new IllegalStateException("Unexpected value: " + operation);
        }
        ;
    }

    /**
     * resolves the create query and identifies if its a database or table creation.
     * 
     * @param queryString
     * @throws IOException
     */
    public void parseCreate(String queryString) throws IOException {
        String[] queryParts = queryString.split(" ");
        if (queryParts.length >= 3 && "DATABASE".equalsIgnoreCase(queryParts[1])) {
            parseCreateDatabase(queryParts[2], queryString);
        } else if (queryParts.length >= 3 && "TABLE".equalsIgnoreCase(queryParts[1])) {
            parseCreateTable(queryString);
        }
    }

    /**
     * executes create database query
     * 
     * @param databaseName
     * @param queryString
     * @throws IOException
     */
    public void parseCreateDatabase(String databaseName, String queryString) throws IOException {
        if (Objects.equals(databaseName, "")) {
            System.out.print("Error: Database name not provided!");
            return;
        } else if (!dbs.isEmpty()) {
            System.out.print("Error: Database already exists, can't create new one!!");
            return;
        }

        String databaseId = String.valueOf(UUID.randomUUID());
        Database db = new Database(databaseId, databaseName);
        this.auth.setCurrentDatabase(db);
        DatabaseManager.saveDatabase(db);
        new Logger("CREATE DATABASE", this.auth.user, databaseName, queryString);
    }

    /**
     * handles begin transaction by applying locking and duplication logic.
     * 
     * @param queryString
     * @throws Exception
     */
    public void parseBeginTransaction(String queryString) throws Exception {
        String[] queryParts = queryString.split(" ");
        if (queryParts.length == 2 && "TRANSACTION".equalsIgnoreCase(queryParts[1])) {
            String DB_COPY_PATH = "tables_copy.txt";
            // copy all the records from original db to copy db
            DatabaseManager.copyFile(this.auth.DB_FILE_PATH, DB_COPY_PATH, false);
            this.auth.DB_FILE_PATH = DB_COPY_PATH;
            this.auth.setCurrentDatabase(this.auth.getCurrentDatabase());
            System.out.println("Transaction has started. To end please write `END TRANSACTION;`");
            new Logger("BEGIN TRANSACTION", this.auth.user, this.auth.getCurrentDatabase().getDatabaseName(),
                    queryString);
        } else {
            System.out.println("Error: Invalid Statement");
        }
    }

    /**
     * copies the data from copy to original file and commits the transaction
     * 
     * @param queryString
     * @throws Exception
     */
    public void parseCommit(String queryString) throws Exception {
        String[] queryParts = queryString.split(" ");
        if (queryParts.length == 1) {
            String DB_COPY_PATH = "tables.txt";
            DatabaseManager.copyFile(this.auth.DB_FILE_PATH, DB_COPY_PATH, false);
            System.out.println("Transactions committed successfully!!");
            new Logger("COMMIT", this.auth.user, this.auth.getCurrentDatabase().getDatabaseName(), queryString);
        }
    }

    /**
     * rollbacks all the transactions
     * 
     * @param queryString
     * @throws Exception
     */
    public void parseRollback(String queryString) throws Exception {
        String[] queryParts = queryString.split(" ");
        if (queryParts.length == 1) {
            String DB_COPY_PATH = "tables_copy.txt";
            DatabaseManager.copyFile("tables.txt", DB_COPY_PATH, false);
            this.auth.DB_FILE_PATH = DB_COPY_PATH;
            this.auth.setCurrentDatabase(this.auth.getCurrentDatabase());
            System.out.println("Transactions rolled back successfully!!");
            new Logger("ROLLBACK", this.auth.user, this.auth.getCurrentDatabase().getDatabaseName(), queryString);
        }
    }

    /**
     * auto commits everything with the transaction block
     * 
     * @param queryString
     * @throws Exception
     */
    public void parseEndTransaction(String queryString) throws Exception {
        String[] queryParts = queryString.split(" ");
        if (queryParts.length == 2 && "TRANSACTION".equalsIgnoreCase(queryParts[1])) {
            String DB_COPY_PATH = "tables.txt";
            // copy all the records from original db to copy db
            DatabaseManager.copyFile(this.auth.DB_FILE_PATH, DB_COPY_PATH, true);
            this.auth.DB_FILE_PATH = DB_COPY_PATH;
            this.auth.setCurrentDatabase(this.auth.getCurrentDatabase());
            System.out.println("Transaction ended!!");
            new Logger("END TRANSACTION", this.auth.user, this.auth.getCurrentDatabase().getDatabaseName(),
                    queryString);
        } else {
            System.out.println("Error: Invalid Statement");
        }
    }

    /**
     * parses query and creates table
     * 
     * @param queryString
     * @throws IOException
     */
    public void parseCreateTable(String queryString) throws IOException {
        Pattern pattern = Pattern.compile("CREATE\\s+TABLE\\s+(\\w+)\\s*\\(([^)]+)\\)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(queryString);
        if (!matcher.matches()) {
            System.out.println("Invalid CREATE TABLE query.");
            return;
        }

        String tableName = matcher.group(1);
        String columnsAndTypes = matcher.group(2);
        FileWriter fileWriter = new FileWriter(this.auth.DB_FILE_PATH, true);
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        PrintWriter printWriter = new PrintWriter(bufferedWriter);
        printWriter.println("startTable - " + tableName);
        printWriter.println("meta: " + columnsAndTypes);
        printWriter.println("endTable - " + tableName + "\n");
        printWriter.close();
        bufferedWriter.close();
        fileWriter.close();
        System.out.println("Table created successfully.");
        new Logger("CREATE TABLE", this.auth.user, this.auth.selectedDatabase.getDatabaseName(), queryString);
    }

    /**
     * parses query and inserts record in table
     * 
     * @param queryString
     * @throws IOException
     */
    private void parseInsert(String queryString) throws IOException {
        Pattern pattern = Pattern.compile("INSERT INTO (\\w+) \\((.*?)\\) VALUES \\((.*?)\\)",
                Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(queryString);
        if (!matcher.find()) {
            System.out.println("Invalid INSERT query.");
            return;
        }

        String tableName = matcher.group(1);
        String values = matcher.group(3);
        BufferedReader bufferedReader = new BufferedReader(new FileReader(this.auth.DB_FILE_PATH));
        StringBuilder fileContent = new StringBuilder();
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            fileContent.append(line).append("\n");
        }

        String startTableDelimiter = "startTable - " + tableName;
        String endTableDelimiter = "endTable - " + tableName;
        int startTableIndex = fileContent.indexOf(startTableDelimiter + "\n" + tableName + ":");
        int endTableIndex = fileContent.indexOf(endTableDelimiter, startTableIndex);
        fileContent.insert(endTableIndex, values + "\n");
        FileWriter fileWriter = new FileWriter(this.auth.DB_FILE_PATH);
        PrintWriter printWriter = new PrintWriter(fileWriter);
        printWriter.print(fileContent.toString());
        bufferedReader.close();
        printWriter.close();
        fileWriter.close();
        System.out.println("Record added successfully.");
        new Logger("INSERT", this.auth.user, this.auth.selectedDatabase.getDatabaseName(), tableName,
                queryString).log();
    }

    /**
     * parses query and performs fetching operation from database
     * 
     * @param queryString
     * @throws Exception
     */
    public void parseSelect(String queryString) throws Exception {
        String tablePattern = "FROM\\s+(\\w+)";
        String columnPattern = "SELECT\\s+(.*?)\\s+FROM";
        String conditionPattern = "\\bWHERE\\b\\s*(.*)$";
        Pattern tablePatternRegex = Pattern.compile(tablePattern, Pattern.CASE_INSENSITIVE);
        Pattern columnPatternRegex = Pattern.compile(columnPattern, Pattern.CASE_INSENSITIVE);
        Pattern conditionPatternRegex = Pattern.compile(conditionPattern, Pattern.CASE_INSENSITIVE);

        Matcher tableMatcher = tablePatternRegex.matcher(queryString);
        if (tableMatcher.find()) {
            String tableName = tableMatcher.group(1);
            String columns = null;
            String whereClause = null;
            Matcher columnMatcher = columnPatternRegex.matcher(queryString);
            if (columnMatcher.find()) {
                columns = columnMatcher.group(1).trim();
            }
            Matcher conditionMatcher = conditionPatternRegex.matcher(queryString);
            if (conditionMatcher.find()) {
                whereClause = conditionMatcher.group(1).trim();
            }
            if (this.auth.selectedDatabase.isValidTable(tableName)) {
                Table table = this.auth.selectedDatabase.fetchTable(tableName);
                List<Map<String, Object>> returnValues = table.getTableValues(whereClause);

                System.out.println("Table: " + tableName);
                if (columns.equals("*")) {
                    columns = table.getColumns().keySet().toString();
                    columns = columns.substring(1, columns.length() - 1);
                }
                System.out.println(columns);
                String[] cols = columns.split(",");
                for (Map<String, Object> row : returnValues) {
                    for (int i = 0; i < cols.length - 1; i++) {
                        System.out.print(row.get(cols[i].trim()) + ",");
                    }
                    System.out.print(row.get(cols[cols.length - 1].trim()) + "\n");
                }
                new Logger("SELECT", this.auth.user, this.auth.selectedDatabase.getDatabaseName(), tableName,
                        queryString).log();
            } else {
                throw new Exception("Invalid Table");
            }
        }
    }

    /**
     * updates the records in the table by parsing query
     * 
     * @param queryString
     * @throws Exception
     */
    public void parseUpdate(String queryString) throws Exception {
        Pattern pattern = Pattern.compile("\\bUPDATE\\b\\s*(\\w+)\\s*SET\\s*(.*?)\\s*\\bWHERE\\b\\s*(.*)$",
                Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(queryString);
        if (matcher.find()) {
            String tableName = matcher.group(1);
            String setClause = matcher.group(2);
            String whereClause = matcher.group(3);
            if (whereClause != null && !whereClause.isEmpty()) {
                if (setClause != null && !setClause.isEmpty()) {
                    Map<String, String> setValues = new HashMap<String, String>();
                    String[] setData = setClause.split(",");
                    for (String data : setData) {
                        String[] columnValues = data.split("=");
                        setValues.put(columnValues[0].trim(), columnValues[1].trim());
                    }
                    if (this.auth.getCurrentDatabase().isValidTable(tableName)) {
                        Table table = this.auth.getCurrentDatabase().fetchTable(tableName);
                        String[] columns = table.getColumns().keySet().toArray(new String[0]);
                        DatabaseManager.updateRecordInTable(this.auth, tableName, columns, whereClause, setValues);
                        this.auth.selectedDatabase.prepareDatabase(this.auth.DB_FILE_PATH);
                        new Logger("UPDATE", this.auth.user, this.auth.selectedDatabase.getDatabaseName(), tableName,
                                queryString).log();
                    } else {
                        throw new Exception("Invalid Table");
                    }
                } else {
                    System.out.println("Invalid Update Statement");
                    throw new Exception("Invalid Update Statement");
                }
            } else {
                System.out.println("Invalid Update Statement. Bulk Update not supported");
                throw new Exception("Invalid Update Statement. Bulk Update not supported");
            }
        }
    }

    /**
     * deletes the given record in the query
     * 
     * @param queryString
     * @throws Exception
     */
    public void parseDelete(String queryString) throws Exception {
        Pattern pattern = Pattern.compile("\\bDELETE\\b\\s*FROM\\s*(\\w+)\\s*\\bWHERE\\b\\s*(.*)$",
                Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(queryString);
        if (matcher.find()) {
            String tableName = matcher.group(1);
            if (this.auth.getCurrentDatabase().isValidTable(tableName)) {
                String whereClause = matcher.group(2);
                if (whereClause != null && !whereClause.isEmpty()) {
                    Table table = this.auth.getCurrentDatabase().fetchTable(tableName);
                    String[] columns = table.getColumns().keySet().toArray(new String[0]);
                    DatabaseManager.deleteRecordInTable(this.auth, tableName, columns, whereClause);
                    table.deleteRecord(whereClause);
                    this.auth.selectedDatabase.prepareDatabase(this.auth.DB_FILE_PATH);
                    new Logger("DELETE", this.auth.user, this.auth.selectedDatabase.getDatabaseName(), tableName,
                            queryString).log();
                } else {
                    System.out.println("Bulk delete is not supported");
                    throw new Exception("Bulk delete is not supported");
                }
            } else {
                throw new Exception("Table not found!");
            }
        } else {
            System.out.println("Invalid DELETE statement.");
            throw new Exception("Invalid DELETE statement.");
        }
    }
}
