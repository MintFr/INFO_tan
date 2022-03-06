import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;

public class MainTanData {
    
    public static LinkedList<String> findLineStops(String route_id,String direction_id)throws SQLException
    {
        LinkedList<String> lineStops=new LinkedList<>();
        // connection to db
        LocalDB co = new LocalDB();
        String trip_id="";
        
        String query1="SELECT trip_id FROM trip WHERE route_id=? AND direction_id=? LIMIT 1";
        PreparedStatement stmt1 = co.getConnect().prepareStatement(query1);
        stmt1.setString(1,route_id);
        stmt1.setString(2,direction_id);
        ResultSet res1 = stmt1.executeQuery();
         while(res1.next())
        {
            trip_id=res1.getString("trip_id");
            String query2="SELECT stop_id FROM stop_times WHERE trip_id=? ORDER BY arrival_times";
            PreparedStatement stmt2 = co.getConnect().prepareStatement(query2);
            stmt2.setString(1,trip_id);
            ResultSet res2 = stmt2.executeQuery();
            while(res2.next())
            {
                String val=res2.getString("stop_id");
                lineStops.add(val);
            }
        }
        
        
       
        return lineStops;  
    }
    
    public static void findStopsCoordinate(String stop_id)throws SQLException
    {
        LocalDB co = new LocalDB();
        String query3="SELECT* FROM stops Where stop_id=?";
        PreparedStatement stmt3 = co.getConnect().prepareStatement(query3);
        stmt3.setString(1,stop_id);
        ResultSet res3 = stmt3.executeQuery();
        while(res3.next())
        {
            System.out.println(" stop_id: "+res3.getString("stop_id")+" stop_lat: "+res3.getString("stop_lat")+" stop_lon: "+res3.getString("stop_lon"));

        }
            
        
        
        
    }
    

    public static void main(String[] args) throws SQLException {
        
        System.out.println("Direction0: "+findLineStops("1-0","0"));

        for (String s:findLineStops("1-0","0"))
        {
            findStopsCoordinate(s);
            
        }
            
        
//        PreparedStatement stmt2;
//        String query2;
//        String routeId;
//        String tripId;
//        // connection to db
//        LocalDB co = new LocalDB();
//
//
//        LinkedList<String> stops2 = new LinkedList<>();
//
//        //String query = "SELECT route_id from routes";
//        String query="SELECT stop_id FROM stop_times WHERE trip_id LIKE '26366121-CR_21_22-HT22H201-L-Ma-Me-J-04' ORDER BY arrival_times";
//        
//
//        PreparedStatement stmt = co.getConnect().prepareStatement(query);
//
//
//        ResultSet res = stmt.executeQuery();
//        while(res.next())
//        {
//            //System.out.println("stop_id: "+res.getString("stop_id"));
//            String val=res.getString("stop_id");
//            stops2.add(val);
//            query2="SELECT* FROM stops Where stop_id=?";
//            stmt2 = co.getConnect().prepareStatement(query2);
//            stmt2.setString(1,val);
//            ResultSet res2 = stmt2.executeQuery();
//            while(res2.next())
//            {
//                System.out.println(" stop_id: "+res2.getString("stop_id")+" stop_lat: "+res2.getString("stop_lat")+" stop_lon: "+res2.getString("stop_lon"));
//                
//            }
//        }
//        System.out.println(stops2);
        
        /***************************************/
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
        /*
        routeIds.next();
        routeId = routeIds.getString("route_id");  // One line of bus/tram
        System.out.println(routeId);
        query2 = "SELECT trip_id FROM trip WHERE route_id=? LIMIT 1";

        stmt2 = co.getConnect().prepareStatement(query2);
        stmt2.setString(1, routeId);
        ResultSet trip = stmt2.executeQuery();
        tripId = trip.getString("trip_id");
        System.out.println(tripId);
        */
    }
}
