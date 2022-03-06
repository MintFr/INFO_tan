import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MainTanData {

    public static void main(String[] args) throws SQLException {
        PreparedStatement stmt2;
        String query2;
        String routeId;
        String tripId;
        String stop_id;
        String stop_lat;
        String stop_lon;
        // connection to db
        LocalDB co = new LocalDB();


        String query = "SELECT stop_id, stop_lat, stop_lon FROM stops";

        PreparedStatement stmt = co.getConnect().prepareStatement(query);

        ResultSet stops = stmt.executeQuery();

        while (stops.next()){
            stop_id = stops.getString("stop_id");
            stop_lat = stops.getString("stop_lat");
            stop_lon = stops.getString("stop_lon");


            System.out.println(stop_id + stop_lat + stop_lon);

        }


        query = "SELECT route_id from routes";

        stmt = co.getConnect().prepareStatement(query);


        ResultSet routeIds = stmt.executeQuery();
        
        while (routeIds.next()){

            routeId = routeIds.getString("route_id");  // One line of bus/tram
            System.out.println(routeId);

            query2 = "SELECT trip_id FROM trip WHERE route_id=? LIMIT 1";

            stmt2 = co.getConnect().prepareStatement(query2);
            stmt2.setString(1, routeId);
            ResultSet trip = stmt2.executeQuery();
            trip.next();
            tripId = trip.getString("trip_id");
            System.out.println(tripId);
        }
    }
}
