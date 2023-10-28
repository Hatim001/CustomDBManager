import utils.Query;
import account.User;
import models.UserManager;
import account.Authentication;

import java.util.List;
import java.util.Scanner;
import java.util.ArrayList;

public class Main {

    private static final Scanner globalScanner = new Scanner(System.in);
    private static final Authentication auth = new Authentication();
    private static final List<User> users = UserManager.fetchUsers();

    private static User currentUser = null;

    /**
     * Entry point for the program.
     * 
     * @param args - Command line arguments
     */
    public static void main(String[] args) throws Exception {
        System.out.print(
                """
                        1. REGISTER
                        2. LOGIN
                        3. EXIT
                        Please select one of the above to proceed:
                        """);
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("input>");
            String command = scanner.nextLine().trim();
            switch (command) {
                case "1":
                    register();
                    break;
                case "":
                    continue;
                case "2":
                    login();
                    break;
                case "3":
                    System.out.println("Goodbye!");
                    System.exit(0);
                default:
                    System.out.println("Invalid command. Supported commands: CREATE USER, LOGIN, EXIT");
            }
        }
    }

    /**
     * Register the user with Auth. If registration fails no action is taken and
     * null is returned.
     */
    private static void register() throws Exception {
        currentUser = auth.register(globalScanner);
        if (currentUser == null)
            return;
        processQuery();
    }

    /**
     * Login to the database. If the user is logged in
     * process the query
     */
    public static void login() throws Exception {
        System.out.print("Please enter your login credentials>>");
        currentUser = auth.login(globalScanner, users);
        if (currentUser == null)
            return;
        processQuery();
    }

    /**
     * Processes queries from the user and creates a Query object to be used in the
     * execution of the query
     */
    public static void processQuery() throws Exception {
        Scanner scanner = new Scanner(System.in);
        List<String> queryInput = new ArrayList<>();
        System.out.println("Authentication Successful. Enter SQL queries or 'EXIT;' to exit the console.");

        while (true) {
            System.out.print("query> ");
            while (true) {
                String line = scanner.nextLine();
                if (line.endsWith(";")) {
                    queryInput.add(line);
                    break;
                }
                queryInput.add(line);
            }

            if ("EXIT;".equalsIgnoreCase(queryInput.get(queryInput.size() - 1).trim())) {
                System.out.println("Goodbye!");
                System.exit(0);
            }

            new Query(queryInput, auth);
            queryInput.clear();
        }
    }
}