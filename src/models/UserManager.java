package models;

import account.User;

import java.io.*;
import java.util.List;
import java.util.ArrayList;

public class UserManager {

    private static final String FILE_PATH = "users.txt";
    private static final String USER_SEPARATOR = "|";

    /**
     * Fetches list of users from file.
     * 
     * @return List of users or empty list if file doesn't exist or
     *         cannot be read for some reason e. g. if no users
     */
    public static List<User> fetchUsers() {
        File file = new File(FILE_PATH);
        // Returns an empty list if the file does not exist.
        if (!file.exists()) {
            return new ArrayList<>();
        }

        List<User> users = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(FILE_PATH))) {
            String line;
            // Read a user from the reader.
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\|");

                // Add a user to the list of users.
                if (parts.length == 3) {
                    User user = new User(parts[0], parts[1], parts[2]);
                    users.add(user);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return users;
    }

    /**
     * Saves the user to file.
     * 
     * @param user - The user to save to file. User must have at least userId
     *             username
     */
    public static void saveUsers(User user) throws IOException {
        try (PrintWriter writer = new PrintWriter(new FileWriter(FILE_PATH, true))) {
            writer.println(
                    user.getUserId() + USER_SEPARATOR + user.getUsername() + USER_SEPARATOR + user.getPassword());
        }
    }
}
