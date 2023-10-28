package utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.time.LocalDateTime;
import java.util.UUID;

import account.User;

public class Logger {
    private static final String LOG_FILE_PATH = "transactions.log";

    public User user;
    public String tableName;
    public String QUERY_TYPE;
    public String queryString;
    public String databaseName;

    // for select, update, delete, insert query
    public Logger(String type, User user, String databaseName, String tableName, String queryString) {
        this.user = user;
        this.QUERY_TYPE = type;
        this.tableName = tableName;
        this.queryString = queryString;
        this.databaseName = databaseName;
    }

    // create database & table query
    public Logger(String type, User user, String databaseName, String queryString) {
        this.user = user;
        this.QUERY_TYPE = type;
        this.queryString = queryString;
        this.databaseName = databaseName;
    }

    // user login and register
    public Logger(String type, User user) {
        this.user = user;
        this.QUERY_TYPE = type;
        this.queryString = "USER " + type;
    }

    /**
    * Writes the transaction log to the log file.
    */
    public void log() {
        String username = String.valueOf(user.getUsername());
        String timestamp = LocalDateTime.now().toString();
        String transactionId = String.valueOf(UUID.randomUUID());
        String logText = String.format("%s | %s | %s | %s | %s | %s %n", transactionId, timestamp, username,
                        this.databaseName, this.tableName, queryString);;

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(LOG_FILE_PATH, true))) {
            writer.write(logText);
        } catch (Exception e) {
            System.out.println("Error while writing transaction log");
        }
    }
}
