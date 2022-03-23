package org.infopgrou;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionDB {
    private Connection connect;

    /**
     * Connect to database containing data of the chosen mean of transportation
     *
     * @param passwd   password for database connection
     * @param username username for database connection
     * @param dbName   name of the database
     * @param address  address of the database
     * @param myLog    logger
     */
    public ConnectionDB(String passwd, String username, String dbName, String address, Log myLog) {
        this.connectToDB(passwd, username, dbName, address, myLog);
    }


    /**
     * Connect to database depending on the requested mean of transportation
     *
     * @param passwd   password for database connection
     * @param username username for database connection
     * @param dbName   name of the database
     * @param address  address of the database
     * @param myLog    logger
     */
    public void connectToDB(String passwd, String username, String dbName, String address, Log myLog) {
        try {
            String fullAddress = "jdbc:postgresql://" + address + "/" + dbName;
            this.connect = DriverManager.getConnection(fullAddress, username, passwd);
        } catch (SQLException e) {
            myLog.getLogger().warning("SQLException : " + e.getMessage());
        }
    }


    /**
     * Close database connection
     *
     * @param myLog logger
     */
    public void closeConnection(Log myLog) {
        try {
            this.connect.close();
        } catch (SQLException ex) {
            myLog.getLogger().warning("SQLException : " + ex.getMessage());
        }
    }

    /**
     * Getter to have a connection to a database
     */
    public Connection getConnect() {
        return connect;
    }
}
