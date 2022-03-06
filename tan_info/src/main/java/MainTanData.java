import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MainTanData {
    /**
     * Main method
     *
     * @param args args
     * @throws SQLException problem with SQL statement
     */
    public static void main(String[] args) throws SQLException, ParseException {

        ConnectionDB localCo = new ConnectionDB("admin", "postgres", "gfts");  // local database with TAN data
        ConnectionDB distantCo = new ConnectionDB("admin", "postgres", "routing_pedestrian_test");  // MINT server database

        System.out.println("Insert all stops");
        insertStops(localCo, distantCo);  // Take all stops and put them in the MINT database
        System.out.println("All stop inserted");
        System.out.println("Insert all ways");
        insertAllWays(localCo, distantCo);  // Insert all ways
        System.out.println("All ways inserted");
    }

    /**
     * Insert all stops into ways_vertices_pgr
     *
     * @param localCo   connection with local database with the TAN data
     * @param distantCo connection with mint application database
     * @throws SQLException problem with SQL statement
     */
    public static void insertStops(ConnectionDB localCo, ConnectionDB distantCo) throws SQLException {
        String stop_id;
        String stop_lat;
        String stop_lon;
        PreparedStatement stmt2;
        BigDecimal lon;
        BigDecimal lat;
        String the_geom;

        String query0 = "DELETE FROM ways_vertices_pgr WHERE tan_data";  // clean the database from current tan stops
        PreparedStatement stmt0 = distantCo.getConnect().prepareStatement(query0);
        stmt0.executeUpdate();

        String query1 = "SELECT stop_id, stop_lat, stop_lon FROM stops";  // get information for all stops on local database
        String query2 = "INSERT INTO ways_vertices_pgr(tan_data, station_name, lat, lon, the_geom) VALUES (true, ?, ?, ?, ST_GeomFromText(?))";  // insert stops data into MINT server

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

            stmt2.executeUpdate();
        }
    }

    /**
     * Insert all pairs of stops into ways by going through every line
     *
     * @param localCo   local database with TAN data
     * @param distantCo MINT server database
     * @throws SQLException problem with SQL statement
     */
    public static void insertAllWays(ConnectionDB localCo, ConnectionDB distantCo) throws SQLException, ParseException {
        String routeId;

        String query0 = "DELETE FROM ways_with_pol WHERE tan_data";
        PreparedStatement stmt0 = distantCo.getConnect().prepareStatement(query0);
        stmt0.executeUpdate();  // Clean the database of tan data before filling it again

        String query1 = "SELECT route_id from routes";  // Get all lines possible for TAN
        PreparedStatement stmt1 = localCo.getConnect().prepareStatement(query1);
        ResultSet routeIds = stmt1.executeQuery();

        while (routeIds.next()) {
            routeId = routeIds.getString("route_id");  // One line of bus/tram
            System.out.println(routeId);

            insertWaysOneLine(routeId, "0", localCo, distantCo);
            insertWaysOneLine(routeId, "1", localCo, distantCo);  // add stops on each direction
        }
    }

    /**
     * Insert all ways for one service for a tan line
     *
     * @param routeId     line of a mean of transportation
     * @param directionId 1 or 0, one way or another
     * @param localCo     connection to the database with TAN data
     * @param distantCo   MINT server database
     * @throws SQLException problem with SQL statements
     */
    public static void insertWaysOneLine(String routeId, String directionId, ConnectionDB localCo, ConnectionDB distantCo) throws SQLException, ParseException {
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
        tripId = res1.getString("trip_id");

        String query2 = "SELECT stop_id FROM stop_times WHERE trip_id=? ORDER BY arrival_times";  // get all stops for the trip_id
        PreparedStatement stmt2 = localCo.getConnect().prepareStatement(query2);
        stmt2.setString(1, tripId);
        ResultSet res2 = stmt2.executeQuery();

        res2.next();  // get the first stop
        stopPrevious = res2.getString("stop_id");
        timePrevious = res2.getString("arrival_times");

        while (res2.next()) {
            stopCurrent = res2.getString("stop_id");
            timeCurrent = res2.getString("arrival_times");

            insertAWay(stopPrevious, timePrevious, stopCurrent, timeCurrent, distantCo);  // Insert a way between the two stops

            timePrevious = timeCurrent;
            stopPrevious = stopCurrent;
        }
    }

    /**
     * Insert a way between two stops
     *
     * @param stopPrevious previous stop tan id
     * @param timePrevious time of arrival for previous stop
     * @param stopCurrent  current stop tan id
     * @param timeCurrent  time of arrival for current stop
     * @param distantCo    connection to the MINT server database
     * @throws ParseException time format not correct
     * @throws SQLException   SQL statement not correct
     */
    public static void insertAWay(String stopPrevious, String timePrevious, String stopCurrent, String timeCurrent, ConnectionDB distantCo) throws ParseException, SQLException {
        DateFormat dateFormat = new SimpleDateFormat("hh:mm:ss");

        Date tPrevious = dateFormat.parse(timePrevious);
        Date tCurrent = dateFormat.parse(timeCurrent);

        long timeDifference = tCurrent.getTime() - tPrevious.getTime();  // difference in millisecondes
        timeDifference = timeDifference / 1000;  // go to secondes

        if (timeDifference < 0) {  // if the current is after midnight and previous is before
            timeDifference = timeDifference + 86400;  // add 86400 = number of seconds in a day
        }

        // get data from both stops
        String query1 = "SELECT id, lon, lat FROM ways_vertices_pgr WHERE station_name = ? AND tan_data";
        PreparedStatement stmt = distantCo.getConnect().prepareStatement(query1);

        // Current stop (target)
        stmt.setString(1, stopCurrent);
        ResultSet res1 = stmt.executeQuery();
        res1.next();
        int currentId = res1.getInt("id");
        double currentLon = res1.getBigDecimal("lon").doubleValue();
        double currentLat = res1.getBigDecimal("lat").doubleValue();

        // Previous stop (source)
        stmt.setString(1, stopPrevious);
        ResultSet res2 = stmt.executeQuery();
        res2.next();
        int previousId = res2.getInt("id");
        double previousLon = res1.getBigDecimal("lon").doubleValue();
        double previousLat = res1.getBigDecimal("lat").doubleValue();
        String the_geom = "LINESTRING(" + previousLon + " " + previousLat + "," + currentLon + " " + previousLat + ")";

        String query2 = "INSERT INTO ways_with_pol(source, target, cost_fast, one_way, oneway, x1, y1, x2, y2, tan_data, the_geom) " +
                "VALUES (?, ?, ?, 1, 'YES', ?, ?, ?, ?, true, ST_GeomFromText(?))";
        PreparedStatement stmt2 = distantCo.getConnect().prepareStatement(query2);
        stmt2.setInt(1, previousId);  // source
        stmt2.setInt(2, currentId);  // target
        stmt2.setDouble(3, timeDifference);  // cost in seconds
        stmt2.setDouble(4, previousLon);
        stmt2.setDouble(5, previousLat);
        stmt2.setDouble(6, currentLon);
        stmt2.setDouble(7, currentLat);
        stmt2.setString(8, the_geom);

        stmt2.executeUpdate();
    }
}
