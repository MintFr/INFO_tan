package org.infopgrou;

import org.json.JSONException;
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
    public static final String SQL_ERROR = "PSQLException : ";
    public static final String STOP_ID = "stop_id";

    /**
     * Main method
     *
     * @param args args
     * @throws SQLException   problem with SQL statement
     * @throws ParseException parse error for timeDifference
     * @throws IOException    error with the logger
     * @throws JSONException  can't get url
     */
    public static void main(String[] args) throws SQLException, ParseException, IOException, JSONException {
        String fileSuffix = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        Log myLog = new Log("logs/log_" + fileSuffix + ".txt");

        ResourceBundle parameters = ResourceBundle.getBundle("credentials");  // get passwords, usernames, addresses, and database name
        myLog.getLogger().info("Connection to local database");
        ConnectionDB localCo = new ConnectionDB(parameters.getString("pwdLocal"), parameters.getString("userLocal"), parameters.getString("dbNameLocal"), parameters.getString("addressLocal"), myLog);  // local database with TAN data
        myLog.getLogger().info("Connected to local database");

        // Code to load data from TAN API
        DataLoader dataLoader = new DataLoader("https://data.nantesmetropole.fr/api/records/1.0/search/?dataset=244400404_tan-arrets-horaires-circuits&q=");
        System.out.println(dataLoader.getUrl());
        String zipFilePath = "data/gtfs-tan.zip";
        String destDir = "data";
        dataLoader.downloadAndUnzip(zipFilePath, destDir);

        // code to put TAN data into local database
        Reader rd = new Reader("data", parameters.getString("userLocal"), parameters.getString("pwdLocal"), "jdbc:postgresql://" + parameters.getString("addressLocal") + "/" + parameters.getString("dbNameLocal"));
        rd.readCalendar();
        rd.readCalendarDates();
        rd.readStop();
        rd.readRoutes();
        rd.readTrips();
        rd.readStopTimes();

        // code to put all TAN data from local database to distant MINT database
        myLog.getLogger().info("Connection to distant database");
        ConnectionDB distantCo = new ConnectionDB(parameters.getString("pwdDistant"), parameters.getString("userDistant"), parameters.getString("dbNameDistant"), parameters.getString("addressDistant"), myLog);  // MINT server database
        myLog.getLogger().info("Connected to distant database");

        cleanData(distantCo, myLog);  // clean the tan data from the MINT database

        insertStops(localCo, distantCo, myLog);  // Take all stops and put them in the MINT database. Link them with the existing nodes

        insertAllWays(localCo, distantCo, myLog);  // Insert all ways between TAN stops

        // close connections to both database
        localCo.closeConnection(myLog);
        distantCo.closeConnection(myLog);
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
        try (PreparedStatement stmt0 = distantCo.getConnect().prepareStatement(query0)) {
            stmt0.executeUpdate();  // Clean the database of tan data before filling it again
            myLog.getLogger().info("Data deleted from ways_with_pol");
        } catch (PSQLException e) {
            myLog.getLogger().warning(SQL_ERROR + e.getMessage());
            myLog.getLogger().warning("Data not deleted from ways_with_pol");
        }

        myLog.getLogger().info("Deleting data from ways_vertices_pgr");
        String query1 = "DELETE FROM ways_vertices_pgr WHERE tan_data; UPDATE ways_vertices_pgr SET tan_data=false";  // clean the database from current tan stops
        try (PreparedStatement stmt1 = distantCo.getConnect().prepareStatement(query1)) {
            stmt1.executeUpdate();
            myLog.getLogger().info("Data deleted from ways_vertices_pgr");
        } catch (PSQLException e) {
            myLog.getLogger().warning(SQL_ERROR + e.getMessage());
            myLog.getLogger().warning("Data not deleted from ways_vertices_pgr");
        }
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
        String stopId;
        String stopLat;
        String stopLon;

        BigDecimal lon;
        BigDecimal lat;

        myLog.getLogger().info("Insert all stops from tan data");

        String query1 = "SELECT stop_id, stop_lat, stop_lon FROM stops";  // get information for all stops on local database

        try (PreparedStatement stmt1 = localCo.getConnect().prepareStatement(query1)) {

            ResultSet stops = stmt1.executeQuery();

            // for all stops
            while (stops.next()) {
                stopId = stops.getString(STOP_ID);
                stopLat = stops.getString("stop_lat");
                lat = new BigDecimal(stopLat);  // adapt for numeric type
                stopLon = stops.getString("stop_lon");
                lon = new BigDecimal(stopLon);

                Stop stopTan = new Stop(stopId, lon, lat);

                stopTan.addStopVertices(distantCo, myLog); // add the stop to MINT database and get the new ID generated

                stopTan.linkWithPedestrianGraph(distantCo, myLog);  // from the stop we create 5 ways to already existing nodes
            }
            myLog.getLogger().info("All stop inserted");
        } catch (PSQLException e) {
            myLog.getLogger().warning(SQL_ERROR + e.getMessage());
            myLog.getLogger().warning("Stops not inserted");
        }
    }


    /**
     * Insert all pairs of stops into ways by going through every line
     *
     * @param localCo   local database with TAN data
     * @param distantCo MINT server database
     * @param myLog     Logger used
     * @throws SQLException   problem with SQL statement
     * @throws ParseException parse error for timeDifference
     */
    public static void insertAllWays(ConnectionDB localCo, ConnectionDB distantCo, Log myLog) throws SQLException, ParseException {
        String routeId;

        myLog.getLogger().info("Insert all ways for TAN services");

        String query1 = "SELECT route_id from routes";  // Get all lines possible for TAN
        try (PreparedStatement stmt1 = localCo.getConnect().prepareStatement(query1)) {

            ResultSet routeIds = stmt1.executeQuery();

            while (routeIds.next()) {
                routeId = routeIds.getString("route_id");  // One line of bus/tram
                myLog.getLogger().info(routeId);

                // add stops on each direction
                insertWaysOneLine(routeId, "0", localCo, distantCo, myLog);
                insertWaysOneLine(routeId, "1", localCo, distantCo, myLog);
            }
            myLog.getLogger().info("All ways inserted");
        } catch (PSQLException e) {
            myLog.getLogger().warning(SQL_ERROR + e.getMessage());
            myLog.getLogger().warning("All ways not inserted");
        }
    }

    /**
     * Insert all ways for one service for a tan line
     *
     * @param routeId     line of a mean of transportation
     * @param directionId 1 or 0, one way or another
     * @param localCo     connection to the database with TAN data
     * @param distantCo   MINT server database
     * @param myLog       Logger used
     * @throws ParseException parse error for timeDifference
     * @throws SQLException   problem with SQL statements
     */
    public static void insertWaysOneLine(String routeId, String directionId, ConnectionDB localCo, ConnectionDB distantCo, Log myLog) throws SQLException, ParseException {
        // get a trip_id for a service (example : line 2-0)
        String query1 = "SELECT trip_id, trip_headsign FROM trip WHERE route_id=? AND direction_id=? LIMIT 1";
        try (PreparedStatement stmt1 = localCo.getConnect().prepareStatement(query1)) {
            stmt1.setString(1, routeId);
            stmt1.setString(2, directionId);
            ResultSet res1 = stmt1.executeQuery();
            res1.next();  // Go to the first result

            String tripId = res1.getString("trip_id");
            String tripHead = res1.getString("trip_headsign");
            myLog.getLogger().info("For direction " + directionId + " " + tripHead + ", the chosen tripId is " + tripId);

            // get all stops for the trip_id
            insertWaysOneTripId(tripId, tripHead, routeId, localCo, distantCo, myLog);

        } catch (PSQLException e) {
            myLog.getLogger().warning(SQL_ERROR + e.getMessage());
            myLog.getLogger().warning("direction " + directionId + " not inserted");
        }
    }

    /**
     * Get all stops for the trip_id
     *
     * @param tripId    id of the trip for TAN data
     * @param tripHead  direction of the trip
     * @param routeId   id of the route
     * @param localCo   local TAN database
     * @param distantCo MINT database
     * @param myLog     logger
     * @throws ParseException error with time difference
     */
    public static void insertWaysOneTripId(String tripId, String tripHead, String routeId, ConnectionDB localCo, ConnectionDB distantCo, Log myLog) throws ParseException {
        String query2 = "SELECT stop_id, arrival_times FROM stop_times WHERE trip_id=? ORDER BY arrival_times";

        try (PreparedStatement stmt = localCo.getConnect().prepareStatement(query2)) {
            stmt.setString(1, tripId);
            ResultSet res = stmt.executeQuery();

            res.next();  // get the first stop

            Way wayAdded = new Way(routeId, tripHead);
            wayAdded.getPrevious().setName(res.getString(STOP_ID));  // id in TAN local database is the name in MINT database
            wayAdded.getPrevious().setArrivalTime(res.getString("arrival_times"));

            wayAdded.getPrevious().fetchFromStationName(distantCo, myLog);  // get the data FROM MINT database using the station_name

            wayAdded.getPrevious().mockStop(distantCo, myLog);  // duplicate the stop and get a new ID for this line only

            while (res.next()) {
                wayAdded.getCurrent().setName(res.getString(STOP_ID)); // id in TAN local database is the name in MINT database
                wayAdded.getCurrent().setArrivalTime(res.getString("arrival_times"));

                wayAdded.getCurrent().fetchFromStationName(distantCo, myLog);  // get the data FROM MINT database using the station_name

                wayAdded.getCurrent().mockStop(distantCo, myLog); // duplicate the stop and get a new ID for this line only

                wayAdded.insertAFullWay(distantCo, myLog);  // Insert a way between the two mock stops

                wayAdded.nextStop();  // copy the current into previous then delete current
            }
        } catch (SQLException e) {
            myLog.getLogger().warning(SQL_ERROR + e.getMessage() +
                    "\nIt is possible that the direction does not exist for this line. Direction was not found or problem with the SQL statement");
        }
    }
}
