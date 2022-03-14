import org.postgresql.util.PSQLException;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.ResourceBundle;


public class MainTanData {
    /**
     * Main method
     *
     * @param args args
     * @throws SQLException problem with SQL statement
     */
    public static void main(String[] args) throws SQLException, ParseException, IOException {
        String fileSuffix = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        Log myLog = new Log("logs/log_" + fileSuffix + ".txt");

        ResourceBundle parameters = ResourceBundle.getBundle("credentials");  // get passwords, usernames, addresses, and database name

        ConnectionDB localCo = new ConnectionDB(parameters.getString("pwdLocal"), parameters.getString("userLocal"), parameters.getString("dbNameLocal"), parameters.getString("addressLocal"));  // local database with TAN data
        ConnectionDB distantCo = new ConnectionDB(parameters.getString("pwdDistant"), parameters.getString("userDistant"), parameters.getString("dbNameDistant"), parameters.getString("addressDistant"));  // MINT server database

        cleanData(distantCo, myLog);  // clean the tan data from the MINT database

        insertStops(localCo, distantCo, myLog);  // Take all stops and put them in the MINT database. Link them with the existing nodes

        insertAllWays(localCo, distantCo, myLog);  // Insert all ways between TAN stops

        localCo.closeConnection();
        distantCo.closeConnection();
    }

    /**
     * Clean TAN data from the MINT database
     *
     * @param distantCo connection with MINT application database
     * @param myLog     logger used
     * @throws SQLException Problem with sql statement
     */
    public static void cleanData(ConnectionDB distantCo, Log myLog) throws SQLException {
        myLog.getLogger().info("Deleting data from ways_with_pol");
        String query0 = "DELETE FROM ways_with_pol WHERE tan_data; UPDATE ways_with_pol SET tan_data=false";
        PreparedStatement stmt0 = distantCo.getConnect().prepareStatement(query0);
        stmt0.executeUpdate();  // Clean the database of tan data before filling it again
        myLog.getLogger().info("Data deleted from ways_with_pol");

        myLog.getLogger().info("Deleting data from ways_vertices_pgr");
        String query1 = "DELETE FROM ways_vertices_pgr WHERE tan_data; UPDATE ways_vertices_pgr SET tan_data=false";  // clean the database from current tan stops
        PreparedStatement stmt1 = distantCo.getConnect().prepareStatement(query1);
        stmt1.executeUpdate();
        myLog.getLogger().info("Data deleted from ways_vertices_pgr");
    }


    /**
     * Insert all stops into ways_vertices_pgr
     *
     * @param localCo   connection with local database with the TAN data
     * @param distantCo connection with mint application database
     * @param myLog     Logger used
     * @throws SQLException problem with SQL statement
     */
    public static void insertStops(ConnectionDB localCo, ConnectionDB distantCo, Log myLog) throws SQLException {
        String stop_id;
        String stop_lat;
        String stop_lon;

        BigDecimal lon;
        BigDecimal lat;

        myLog.getLogger().info("Insert all stops from tan data");

        String query1 = "SELECT stop_id, stop_lat, stop_lon FROM stops";  // get information for all stops on local database

        PreparedStatement stmt1 = localCo.getConnect().prepareStatement(query1);
        ResultSet stops = stmt1.executeQuery();

        // for all stops
        while (stops.next()) {
            stop_id = stops.getString("stop_id");
            stop_lat = stops.getString("stop_lat");
            lat = new BigDecimal(stop_lat);  // adapt for numeric type
            stop_lon = stops.getString("stop_lon");
            lon = new BigDecimal(stop_lon);

            Stop stopTan = new Stop(stop_id, lon, lat);

            stopTan.addStopVertices(distantCo); // add the stop to MINT database and get the new ID generated

            stopTan.linkWithPedestrianGraph(distantCo);  // from the stop we create 5 ways to already existing nodes
        }
        myLog.getLogger().info("All stop inserted");
    }


    /**
     * Insert all pairs of stops into ways by going through every line
     *
     * @param localCo   local database with TAN data
     * @param distantCo MINT server database
     * @param myLog     Logger used
     * @throws SQLException problem with SQL statement
     */
    public static void insertAllWays(ConnectionDB localCo, ConnectionDB distantCo, Log myLog) throws SQLException, ParseException {
        String routeId;

        myLog.getLogger().info("Insert all ways for TAN services");

        String query1 = "SELECT route_id from routes";  // Get all lines possible for TAN
        PreparedStatement stmt1 = localCo.getConnect().prepareStatement(query1);
        ResultSet routeIds = stmt1.executeQuery();

        while (routeIds.next()) {
            routeId = routeIds.getString("route_id");  // One line of bus/tram
            myLog.getLogger().info(routeId);

            // add stops on each direction
            insertWaysOneLine(routeId, "0", localCo, distantCo, myLog);
            insertWaysOneLine(routeId, "1", localCo, distantCo, myLog);
        }
        myLog.getLogger().info("All ways inserted");
    }

    /**
     * Insert all ways for one service for a tan line
     *
     * @param routeId     line of a mean of transportation
     * @param directionId 1 or 0, one way or another
     * @param localCo     connection to the database with TAN data
     * @param distantCo   MINT server database
     * @param myLog       Logger used
     * @throws SQLException problem with SQL statements
     */
    public static void insertWaysOneLine(String routeId, String directionId, ConnectionDB localCo, ConnectionDB distantCo, Log myLog) throws SQLException, ParseException {
        String tripId;
        String tripHead;

        String query1 = "SELECT trip_id, trip_headsign FROM trip WHERE route_id=? AND direction_id=? LIMIT 1";  // get a trip_id for a service (example : line 2-0)
        PreparedStatement stmt1 = localCo.getConnect().prepareStatement(query1);
        stmt1.setString(1, routeId);
        stmt1.setString(2, directionId);
        ResultSet res1 = stmt1.executeQuery();
        res1.next();  // Go to the first result

        try {
            tripId = res1.getString("trip_id");
            tripHead = res1.getString("trip_headsign");
            myLog.getLogger().info("For direction " + directionId + " " + tripHead + ", the chosen tripId is " + tripId);

            String query2 = "SELECT stop_id, arrival_times FROM stop_times WHERE trip_id=? ORDER BY arrival_times";  // get all stops for the trip_id
            PreparedStatement stmt2 = localCo.getConnect().prepareStatement(query2);
            stmt2.setString(1, tripId);
            ResultSet res2 = stmt2.executeQuery();

            res2.next();  // get the first stop

            Way wayAdded = new Way(routeId, tripHead);
            wayAdded.getPrevious().setName(res2.getString("stop_id"));  // id in TAN local database is the name in MINT database
            wayAdded.getPrevious().setArrivalTime(res2.getString("arrival_times"));

            wayAdded.getPrevious().fetchFromStationName(distantCo);  // get the data FROM MINT database using the station_name

            

            while (res2.next()) {
                wayAdded.getCurrent().setName(res2.getString("stop_id")); // id in TAN local database is the name in MINT database
                wayAdded.getCurrent().setArrivalTime(res2.getString("arrival_times"));

                wayAdded.getCurrent().fetchFromStationName(distantCo);  // get the data FROM MINT database using the station_name

                wayAdded.insertAFullWay(distantCo);  // Insert a way between the two stops

                wayAdded.nextStop();  // copy the current into previous then delete current
            }
        } catch (PSQLException e) {
            myLog.getLogger().warning("PSQLException : " + e.getMessage() +
                    "\nIt is possible that the direction does not exist for this line. Direction was not found or problem with the SQL statement");
        }
    }
}
