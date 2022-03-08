import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionDB {
    private Connection connect;

    /**
     * Connect to database containing datas of the chosen mean of transportation
     * @param passwd password for database connection
     * @param username username for database connection
     * @param dbName name of the database
     * @param address address of the database
     */
    public ConnectionDB(String passwd, String username, String dbName, String address) {
        this.connectToDB(passwd, username, dbName, address);
    }


    /**
     * Connect to database depending on the requested mean of transportation
     * @param passwd password for database connection
     * @param username username for database connection
     * @param dbName name of the database
     * @param address address of the database
     */
    public void connectToDB(String passwd, String username, String dbName, String address) {
        try {
            Class.forName("org.postgresql.Driver");
            String fullAddress = "jdbc:postgresql://" + address + "/" + dbName;
            this.connect = DriverManager.getConnection(fullAddress, username, passwd);
        } catch (ClassNotFoundException e) {
            System.err.println("ClassNotFoundException : " + e.getMessage());
        } catch (SQLException e) {
            System.err.println("SQLException : " + e.getMessage());
        }
    }


    /**
     * Close database connection
     */
    public void closeConnection() {
        try {
            this.connect.close();
        } catch (SQLException ex) {
            System.err.println("SQLException : " + ex.getMessage());
        }
    }

    /**
     * Getter to have a connection to a database
     */
    public Connection getConnect() {
        return connect;
    }
}
