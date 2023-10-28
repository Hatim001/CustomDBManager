package account;

import models.DatabaseManager;
import models.UserManager;
import utils.Logger;

import java.util.*;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * This class is used for Authenticating user with Multi-Factor
 * authentication. This class extends UserManager Class for accessing
 * user variables
 */
public class Authentication extends UserManager {

    public User user;
    public Database selectedDatabase;
    public String DB_FILE_PATH = "tables.txt";
    private static final int MAX_LOGIN_ATTEMPTS = 3;

    /**
     * registers the user and creates entry in local text file
     * 
     * @param scanner
     * @return User
     * @throws Exception
     */
    public User register(Scanner scanner) throws Exception {
        System.out.println("Register yourself >>>>");
        System.out.print("Enter username: ");
        String username = scanner.nextLine();

        if (User.isUserExists(username)) {
            System.out.println("Username already exists. Please choose another one.");
            return null;
        }

        char[] passwordChars = readPasswordFromConsole();
        String password = new String(passwordChars);
        String passwordHash = null;
        try {
            passwordHash = hashPassword(password);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Please complete the captcha:");
        if (!validateCaptcha(scanner)) {
            System.out.println("Captcha validation failed. Registration aborted.");
            return null;
        }

        String userId = String.valueOf(UUID.randomUUID());
        User newUser = new User(userId, username, passwordHash);
        try {
            saveUsers(newUser);
            this.user = newUser;
            new Logger("REGISTER", newUser).log();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Registration successful.");
        return newUser;
    }

    /**
     * Takes captcha's answer from user and validates it
     * 
     * @param scanner
     * @return boolean
     */
    private static boolean validateCaptcha(Scanner scanner) {
        String captcha = generateCaptcha();
        System.out.println("Captcha: " + captcha);
        System.out.print("Enter the captcha: ");
        String userInput = scanner.nextLine();
        return captcha.equals(userInput);
    }

    /**
     * @return String
     */
    private static String generateCaptcha() {
        return String.valueOf((int) (Math.random() * 9000) + 1000);
    }

    /**
     * logs in the user, by checking if user exists in database or not
     * 
     * @param scanner
     * @param users
     * @return User
     * @throws IOException
     */
    public User login(Scanner scanner, List<User> users) throws IOException {
        System.out.println("\nLogin:");
        System.out.print("Enter username: ");
        String username = scanner.nextLine();

        for (User user : users) {
            if (!Objects.equals(user.getUsername(), username)) {
                continue;
            }
            int attempts = 0;
            while (attempts < MAX_LOGIN_ATTEMPTS) {
                char[] passwordChars = readPasswordFromConsole();
                String password = new String(passwordChars);
                String inputPasswordHash = null;
                try {
                    inputPasswordHash = hashPassword(password);
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException(e);
                }
                if (user.getPassword().equals(inputPasswordHash) && validateCaptcha(scanner)) {
                    System.out.println("Login successful.");
                    this.setCurrentDatabase(DatabaseManager.getDatabaseFromFile());
                    this.user = user;
                    new Logger("LOGIN", user).log();
                    return user;
                } else {
                    System.out.println("Incorrect password. Please try again.");
                    attempts++;
                }
            }
            System.out.println("Too many incorrect attempts. Login aborted.");
        }
        System.out.println("User not found.");
        return null;
    }

    /**
     * common util for reading password from console
     * 
     * @return char[]
     */
    private static char[] readPasswordFromConsole() {
        System.out.print("Enter password: ");
        Scanner scanner = new Scanner(System.in);
        char[] input = scanner.nextLine().toCharArray();
        scanner.close();
        return input;
    }

    /**
     * converts the password into md5 SHA and stores it in the database
     * 
     * @param input
     * @return String
     * @throws NoSuchAlgorithmException
     */
    private static String hashPassword(String input) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] messageDigest = md.digest(input.getBytes());
        StringBuilder hexString = new StringBuilder();
        for (byte b : messageDigest) {
            hexString.append(String.format("%02x", b));
        }

        return hexString.toString();
    }

    /**
     * sets current database every time the user is authenticated
     * 
     * @param selectedDatabase
     */
    public void setCurrentDatabase(Database selectedDatabase) {
        this.selectedDatabase = selectedDatabase;
        if (selectedDatabase != null) {
            selectedDatabase.prepareDatabase(this.DB_FILE_PATH);
        }
    }

    /**
     * returns the current selected database
     * 
     * @return Database
     */
    public Database getCurrentDatabase() {
        return selectedDatabase;
    }
}
