package account;

import models.UserManager;

import java.util.List;

public class User {
    private String username;
    private String password;
    private String userId;

    public User(String userId, String username, String password) {
        this.username = username;
        this.password = password;
        this.userId = userId;
    }

    /**
     * Returns the username associated with this user.
     * 
     * @return the username associated with this user or null if there is no
     *         username
     */
    public String getUsername() {
        return username;
    }

    /**
     * Returns the user ID.
     * 
     * @return the user ID or null if not set by the client.
     */
    public String getUserId() {
        return userId;
    }

    /**
     * Returns the password to use for this authentication.
     * 
     * @return the password to use for this authentication
     */
    public String getPassword() {
        return password;
    }

    /**
     * Checks if a user exists.
     * 
     * @param name - The username of the user
     * 
     * @return true if the user exists
     */
    public static boolean isUserExists(String name) {
        List<User> users = UserManager.fetchUsers();
        for (User user : users) {
            // Check if the user is a valid user name.
            if (user.getUsername().equals(name)) {
                return true;
            }
        }
        return false;
    }
}
