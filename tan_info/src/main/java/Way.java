import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Class for ways between two nodes. Corresponds to ways_with_pol
 */
public class Way {

    private String routeId;
    private String tripHead;
    private Stop current;
    private Stop previous;

    public Way(Stop tanStop) {
        this.previous = new Stop(tanStop);
        this.current = new Stop();
    }

    public Way(String routeId, String tripHead) {
        this.routeId = routeId;
        this.tripHead = tripHead;
        this.current = new Stop();
        this.previous = new Stop();
    }

    public void setCurrent(Stop current) {
        this.current = current;
    }

    public void setPrevious(Stop previous) {
        this.previous = previous;
    }

    public Stop getCurrent() {
        return current;
    }

    public Stop getPrevious() {
        return previous;
    }

    public String getRouteId() {
        return routeId;
    }

    public String getTripHead() {
        return tripHead;
    }

    /**
     * Insert a way between two stops
     *
     * @param distantCo connection to the MINT server database
     * @throws ParseException parse error for timeDifference
     * @throws SQLException   SQL statement not correct
     */
    public void insertAFullWay(ConnectionDB distantCo) throws SQLException, ParseException {
        long timeDiff = this.timeDifference();

        String the_geom = "LINESTRING(" + this.getPrevious().getLon() + " " + this.getPrevious().getLat() + "," + this.getCurrent().getLon() + " " + this.getCurrent().getLat() + ")";

        String query = "INSERT INTO ways_with_pol(source, target, cost_fast, one_way, oneway, x1, y1, x2, y2, source_name, target_name, route_id, trip_head, tan_data, the_geom) " +
                "VALUES (?, ?, ?, 1, 'YES', ?, ?, ?, ?, ?, ?, ?, ?, true, ST_GeomFromText(?))";
        PreparedStatement stmt = distantCo.getConnect().prepareStatement(query);
        stmt.setInt(1, this.getPrevious().getId());  // source
        stmt.setInt(2, this.getCurrent().getId());  // target
        stmt.setDouble(3, timeDiff);  // cost in seconds
        stmt.setDouble(4, this.getPrevious().getLon().doubleValue());
        stmt.setDouble(5, this.getPrevious().getLat().doubleValue());
        stmt.setDouble(6, this.getCurrent().getLon().doubleValue());
        stmt.setDouble(7, this.getCurrent().getLat().doubleValue());
        stmt.setString(8, this.getPrevious().getName());
        stmt.setString(9, this.getCurrent().getName());
        stmt.setString(10, this.getRouteId());
        stmt.setString(11, this.getTripHead());
        stmt.setString(12, the_geom);

        stmt.executeUpdate();
    }

    /**
     * Insert a way between two stops
     *
     * @param distantCo connection to the MINT server database
     * @throws SQLException   SQL statement not correct
     */
    public void insertAWayWithPedestrian(ConnectionDB distantCo) throws SQLException {

        String the_geom = "LINESTRING(" + this.getPrevious().getLon() + " " + this.getPrevious().getLat() + "," + this.getCurrent().getLon() + " " + this.getCurrent().getLat() + ")";

        String query = "INSERT INTO ways_with_pol(source, target, cost_fast, x1, y1, x2, y2, tan_data, the_geom) " +
                "VALUES (?, ?, 30, ?, ?, ?, ?, true, ST_GeomFromText(?))";  // arbitrary cost of 30s for the closest nodes possible

        PreparedStatement stmt2 = distantCo.getConnect().prepareStatement(query);
        stmt2.setInt(1, this.getPrevious().getId());  // source
        stmt2.setInt(2, this.getCurrent().getId());  // target
        stmt2.setDouble(3, this.getPrevious().getLon().doubleValue());
        stmt2.setDouble(4, this.getPrevious().getLat().doubleValue());
        stmt2.setDouble(5, this.getCurrent().getLon().doubleValue());
        stmt2.setDouble(6, this.getCurrent().getLat().doubleValue());
        stmt2.setString(7, the_geom);

        stmt2.executeUpdate();
    }

    /**
     * Calculate time difference between the two stops
     *
     * @return time difference in seconds
     * @throws ParseException error of parsing the datetime
     */
    public long timeDifference() throws ParseException {
        DateFormat dateFormat = new SimpleDateFormat("hh:mm:ss");

        Date tPrevious = dateFormat.parse(this.getPrevious().getArrivalTime());
        Date tCurrent = dateFormat.parse(this.getCurrent().getArrivalTime());

        long timeDiff = tCurrent.getTime() - tPrevious.getTime();  // difference in milliseconds
        timeDiff = timeDiff / 1000;  // go to seconds

        if (timeDiff < 0) {  // if the current is after midnight and previous is before midnight we have negative duration
            timeDiff = timeDiff + 86400;  // add 86400 = number of seconds in a day
        }

        if (timeDiff == 0) {  // TAN can sometimes say that two successive stops have the same arrival_time
            timeDiff = 60;  // it needs at least 1 minute
        }

        return timeDiff;
    }

    /**
     * Get the distance between two stops current and previous using Haversine formula
     *
     * @return distance
     */
    public double distanceLonLat() {
        double R = 6371e3;

        double lon1 = Math.toRadians(this.getCurrent().getLon().doubleValue());
        double lon2 = Math.toRadians(this.getPrevious().getLon().doubleValue());
        double lat1 = Math.toRadians(this.getCurrent().getLat().doubleValue());
        double lat2 = Math.toRadians(this.getPrevious().getLat().doubleValue());

        double dLon = lon2 - lon1;
        double dLat = lat2 - lat1;

        double a = Math.pow(Math.sin(dLat / 2), 2) + Math.cos(lat1) * Math.cos(lat2) * Math.pow(Math.sin(dLon / 2), 2);

        double c = 2 * Math.asin(Math.sqrt(a));

        return (c * R);
    }

    /**
     * Copy the current into previous then delete current
     */
    public void nextStop() {
        this.setPrevious(new Stop(this.getCurrent()));  // copy current into previous
        this.setCurrent(new Stop());
    }
}
