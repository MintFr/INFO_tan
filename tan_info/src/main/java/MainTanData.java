import org.postgresql.util.PSQLException;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
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

        cleanData(localCo, distantCo, myLog);  // clean the tan data from the MINT database

        insertStops(localCo, distantCo, myLog);  // Take all stops and put them in the MINT database. Link them with the existing nodes

        insertAllWays(localCo, distantCo, myLog);  // Insert all ways between TAN stops

        localCo.closeConnection();
        distantCo.closeConnection();
    }

    public static void cleanData(ConnectionDB localCo, ConnectionDB distantCo, Log myLog) throws SQLException {
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
        PreparedStatement stmt2;
        BigDecimal lon;
        BigDecimal lat;
        String the_geom;

        myLog.getLogger().info("Insert all stops from tan data");

        String query1 = "SELECT stop_id, stop_lat, stop_lon FROM stops";  // get information for all stops on local database
        String query2 = "INSERT INTO ways_vertices_pgr(tan_data, station_name, lat, lon, the_geom) VALUES (true, ?, ?, ?, ST_GeomFromText(?)) RETURNING ID";  // insert stops data into MINT server

        PreparedStatement stmt1 = localCo.getConnect().prepareStatement(query1);
        ResultSet stops = stmt1.executeQuery();

        // for all stops
        while (stops.next()) {
            stop_id = stops.getString("stop_id");
            stop_lat = stops.getString("stop_lat");
            lat = new BigDecimal(stop_lat);  // adapt for numeric type
            stop_lon = stops.getString("stop_lon");
            lon = new BigDecimal(stop_lon);
            the_geom = "POINT(" + stop_lon + " " + stop_lat + ")";

            stmt2 = distantCo.getConnect().prepareStatement(query2);
            stmt2.setString(1, stop_id);
            stmt2.setBigDecimal(2, lat);  // adapt for numeric type
            stmt2.setBigDecimal(3, lon);
            stmt2.setString(4, the_geom);

            ResultSet idGenerated = stmt2.executeQuery();  // get the id of the inserted data
            idGenerated.next();
            int idStop = idGenerated.getInt("id");

            linkWithPedestrianGraph(idStop, lon, lat, distantCo);  // from the stop we create 5 ways to already existing nodes
        }
        myLog.getLogger().info("All stop inserted");
    }

    public static void linkWithPedestrianGraph(int idStop, BigDecimal lon, BigDecimal lat, ConnectionDB distantCo) throws SQLException {
        int nodeId;
        BigDecimal nodeLon;
        BigDecimal nodeLat;
        PreparedStatement stmt2;

        // Get a list of close nodes that are not from tan_data : access bus/tram stops as a pedestrian
        String query1 = "SELECT id, lon, lat FROM ways_vertices_pgr WHERE tan_data=false ORDER BY the_geom <-> ST_SetSRID(ST_Point (?, ?),4326) limit 5;";
        PreparedStatement stmt1 = distantCo.getConnect().prepareStatement(query1);
        stmt1.setBigDecimal(1, lon);
        stmt1.setBigDecimal(2, lat);
        ResultSet pedestrianNodes = stmt1.executeQuery();

        String query2 = "INSERT INTO ways_with_pol(source, target, cost_fast, x1, y1, x2, y2, tan_data, the_geom) " +
                "VALUES (?, ?, 40, ?, ?, ?, ?, true, ST_GeomFromText(?))";  // arbitrary cost of 40s for the closest nodes possible

        while (pedestrianNodes.next()) {
            nodeId = pedestrianNodes.getInt("id");
            nodeLon = pedestrianNodes.getBigDecimal("lon");
            nodeLat = pedestrianNodes.getBigDecimal("lat");

            String the_geom = "LINESTRING(" + lon + " " + lat + "," + nodeLon + " " + nodeLat + ")";

            stmt2 = distantCo.getConnect().prepareStatement(query2);

            stmt2.setInt(1, idStop);  // source
            stmt2.setInt(2, nodeId);  // target
            stmt2.setDouble(3, lon.doubleValue());
            stmt2.setDouble(4, lat.doubleValue());
            stmt2.setDouble(5, nodeLon.doubleValue());
            stmt2.setDouble(6, nodeLat.doubleValue());
            stmt2.setString(7, the_geom);

            stmt2.executeUpdate();
        }
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
        String stopCurrent;
        String stopPrevious;
        String timeCurrent;
        String timePrevious;

        String query1 = "SELECT trip_id FROM trip WHERE route_id=? AND direction_id=? LIMIT 1";  // get a trip_id for a service (example : line 2-0)
        PreparedStatement stmt1 = localCo.getConnect().prepareStatement(query1);
        stmt1.setString(1, routeId);
        stmt1.setString(2, directionId);
        ResultSet res1 = stmt1.executeQuery();
        res1.next();  // Go to the first result
        try {
            tripId = res1.getString("trip_id");

            myLog.getLogger().info("For direction " + directionId + " the chosen tripId is " + tripId);

            String query2 = "SELECT stop_id, arrival_times FROM stop_times WHERE trip_id=? ORDER BY arrival_times";  // get all stops for the trip_id
            PreparedStatement stmt2 = localCo.getConnect().prepareStatement(query2);
            stmt2.setString(1, tripId);
            ResultSet res2 = stmt2.executeQuery();

            res2.next();  // get the first stop
            stopPrevious = res2.getString("stop_id");
            timePrevious = res2.getString("arrival_times");

            while (res2.next()) {
                stopCurrent = res2.getString("stop_id");
                timeCurrent = res2.getString("arrival_times");

                insertAWay(stopPrevious, timePrevious, stopCurrent, timeCurrent, routeId, distantCo);  // Insert a way between the two stops

                timePrevious = timeCurrent;
                stopPrevious = stopCurrent;
            }
        } catch (PSQLException e) {
            myLog.getLogger().warning("PSQLException : " + e.getMessage() +
                    "\nIt is possible that the direction does not exist for this line. Direction was not found or problem with the SQL statement");
        }
    }

    /**
     * Insert a way between two stops
     *
     * @param stopPrevious previous stop tan id
     * @param timePrevious time of arrival for previous stop
     * @param stopCurrent  current stop tan id
     * @param timeCurrent  time of arrival for current stop
     * @param routeId      route id for the two stops
     * @param distantCo    connection to the MINT server database
     * @throws ParseException time format not correct
     * @throws SQLException   SQL statement not correct
     */
    public static void insertAWay(String stopPrevious, String timePrevious, String stopCurrent, String timeCurrent, String routeId, ConnectionDB distantCo) throws ParseException, SQLException {
        DateFormat dateFormat = new SimpleDateFormat("hh:mm:ss");

        Date tPrevious = dateFormat.parse(timePrevious);
        Date tCurrent = dateFormat.parse(timeCurrent);

        long timeDifference = tCurrent.getTime() - tPrevious.getTime();  // difference in millisecondes
        timeDifference = timeDifference / 1000;  // go to secondes

        if (timeDifference < 0) {  // if the current is after midnight and previous is before
            timeDifference = timeDifference + 86400;  // add 86400 = number of seconds in a day
        }

        // get data from both stops
        String query1 = "SELECT id, lon, lat, station_name FROM ways_vertices_pgr WHERE station_name = ? AND tan_data";
        PreparedStatement stmt = distantCo.getConnect().prepareStatement(query1);

        // Current stop (target)
        stmt.setString(1, stopCurrent);
        ResultSet res1 = stmt.executeQuery();
        res1.next();
        int currentId = res1.getInt("id");
        BigDecimal currentLon = res1.getBigDecimal("lon");
        BigDecimal currentLat = res1.getBigDecimal("lat");
        String currentName = res1.getString("station_name");

        // Previous stop (source)
        stmt.setString(1, stopPrevious);
        ResultSet res2 = stmt.executeQuery();
        res2.next();
        int previousId = res2.getInt("id");
        BigDecimal previousLon = res2.getBigDecimal("lon");
        BigDecimal previousLat = res2.getBigDecimal("lat");
        String previousName = res2.getString("station_name");

        String the_geom = "LINESTRING(" + previousLon + " " + previousLat + "," + currentLon + " " + currentLat + ")";

        String query2 = "INSERT INTO ways_with_pol(source, target, cost_fast, one_way, oneway, x1, y1, x2, y2, source_name, target_name, route_id, tan_data, the_geom) " +
                "VALUES (?, ?, ?, 1, 'YES', ?, ?, ?, ?, ?, ?, ?, true, ST_GeomFromText(?))";
        PreparedStatement stmt2 = distantCo.getConnect().prepareStatement(query2);
        stmt2.setInt(1, previousId);  // source
        stmt2.setInt(2, currentId);  // target
        stmt2.setDouble(3, timeDifference);  // cost in seconds
        stmt2.setDouble(4, previousLon.doubleValue());
        stmt2.setDouble(5, previousLat.doubleValue());
        stmt2.setDouble(6, currentLon.doubleValue());
        stmt2.setDouble(7, currentLat.doubleValue());
        stmt2.setString(8, previousName);
        stmt2.setString(9, currentName);
        stmt2.setString(10, routeId);
        stmt2.setString(11, the_geom);

        stmt2.executeUpdate();
    }
}
