import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;

public class MainTanData {

    public static void main(String[] args) throws SQLException {
        PreparedStatement stmt2;
        String query2;
        String routeId;
        String tripId;
        // connection to db
        LocalDB co = new LocalDB();


        // LinkedList<String> routeIds = new LinkedList<>();

        String query = "SELECT route_id from routes";

        PreparedStatement stmt = co.getConnect().prepareStatement(query);


        ResultSet routeIds = stmt.executeQuery();
        /*
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
        */
        routeIds.next();
        routeId = routeIds.getString("route_id");  // One line of bus/tram
        System.out.println(routeId);
        query2 = "SELECT trip_id FROM trip WHERE route_id=? LIMIT 1";

        stmt2 = co.getConnect().prepareStatement(query2);
        stmt2.setString(1, routeId);
        ResultSet trip = stmt2.executeQuery();
        tripId = trip.getString("trip_id");
        System.out.println(tripId);

    }
}
