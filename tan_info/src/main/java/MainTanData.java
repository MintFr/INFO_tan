import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;

public class MainTanData {

    /**
     * @param route_id     line of a mean of transportation
     * @param direction_id 1 or 0, one way or another
     * @param co           connection to the database
     * @return get all stops for a trip_id of a route_id
     * @throws SQLException problem with SQL statements
     */
    public static LinkedList<String> findLineStops(String route_id, String direction_id, ConnectionDB co) throws SQLException {
        LinkedList<String> lineStops = new LinkedList<>();
        String trip_id;

        String query1 = "SELECT trip_id FROM trip WHERE route_id=? AND direction_id=? LIMIT 1";  // get a trip_id for a service (example : line 2-0)
        PreparedStatement stmt1 = co.getConnect().prepareStatement(query1);
        stmt1.setString(1, route_id);
        stmt1.setString(2, direction_id);
        ResultSet res1 = stmt1.executeQuery();
        while (res1.next()) {
            trip_id = res1.getString("trip_id");
            String query2 = "SELECT stop_id FROM stop_times WHERE trip_id=? ORDER BY arrival_times";  // get all stops for the trip_id
            PreparedStatement stmt2 = co.getConnect().prepareStatement(query2);
            stmt2.setString(1, trip_id);
            ResultSet res2 = stmt2.executeQuery();
            while (res2.next()) {
                String val = res2.getString("stop_id");
                lineStops.add(val);
            }
        }
        return lineStops;
    }

    /**
     * @param stop_id stop_id from TAN, in 4 letters
     * @param co      connection to the database
     * @throws SQLException problem with SQL statement
     */
    public static void findStopsCoordinate(String stop_id, ConnectionDB co) throws SQLException {
        String query3 = "SELECT * FROM stops WHERE stop_id=?";  // get data for this stop
        PreparedStatement stmt3 = co.getConnect().prepareStatement(query3);
        stmt3.setString(1, stop_id);
        ResultSet res3 = stmt3.executeQuery();
        while (res3.next()) {
            System.out.println(" stop_id: " + res3.getString("stop_id") + " stop_lat: " + res3.getString("stop_lat") + " stop_lon: " + res3.getString("stop_lon"));
        }
    }

    /**
     * Insert all stops into ways_vertices_pgr
     * @param localCo connection with local database with the TAN data
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
        ResultSet res;
        String query1 = "SELECT stop_id, stop_lat, stop_lon FROM stops";
        String query2 = "INSERT INTO ways_vertices_pgr(tan_data, station_name, lat, lon, the_geom) VALUES (true, ?, ?, ?, ST_GeomFromText(?))";


        PreparedStatement stmt1 = localCo.getConnect().prepareStatement(query1);
        ResultSet stops = stmt1.executeQuery();

        while (stops.next()) {
            stop_id = stops.getString("stop_id");
            stop_lat = stops.getString("stop_lat");
            lat = new BigDecimal(stop_lat);
            stop_lon = stops.getString("stop_lon");
            lon = new BigDecimal(stop_lon);
            the_geom = "POINT(" + stop_lon + " " + stop_lat + ")";

            stmt2 = distantCo.getConnect().prepareStatement(query2);
            stmt2.setString(1, stop_id);
            stmt2.setBigDecimal(2, lat);  // adapt for numeric type
            stmt2.setBigDecimal(3, lon);
            stmt2.setString(4, the_geom);

            stmt2.executeUpdate();
            System.out.println(stop_id + stop_lat + stop_lon);
        }
    }

    /**
     * Main method
     *
     * @param args args
     * @throws SQLException problem with SQL statement
     */
    public static void main(String[] args) throws SQLException {

        ConnectionDB localCo = new ConnectionDB("admin", "postgres", "gfts");
        ConnectionDB distantCO = new ConnectionDB("admin", "postgres", "routing_pedestrian_test");

        insertStops(localCo, distantCO);
        System.out.println("Direction0: " + findLineStops("1-0", "0", localCo));

        for (String s : findLineStops("1-0", "0", localCo)) {
            findStopsCoordinate(s, localCo);

        }
    }
}
