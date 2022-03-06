import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class LocalDB {
    private Connection connect;

    /**
     * Connect to database containing datas of the chosen mean of transportation
     */
    public LocalDB(){
        this.connectToDB();
    }


    /**
     * Connect to database depending on the requested mean of transportation
     *
     */
    public void connectToDB() {
        try {
            Class.forName("org.postgresql.Driver");
            String user = "postgres";
            String pwd = "";
            String address = "jdbc:postgresql://localhost/TANgft";

            this.connect = DriverManager.getConnection(address, user, pwd);

        } catch (ClassNotFoundException e) {
            System.err.println("ClassNotFoundException : " + e.getMessage());
        } catch (SQLException e) {
            System.err.println("SQLException : " + e.getMessage()) ;
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
     *
     */
    public Connection getConnect() {
        return connect;
    }

}
