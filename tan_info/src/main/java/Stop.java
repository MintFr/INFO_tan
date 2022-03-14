import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Class for TAN stops. Corresponds to ways_with_vertices
 */
public class Stop {

    private String name;
    private BigDecimal lon;
    private BigDecimal lat;
    private int id;
    private String arrivalTime;

    public Stop() {
    }

    public Stop(String name, BigDecimal lon, BigDecimal lat) {
        this.name = name;
        this.lon = lon;
        this.lat = lat;
    }

    /**
     * Copy constructor
     *
     * @param previous Stop to be copied
     */
    public Stop(Stop previous) {
        this.name = previous.getName();
        this.lon = previous.getLon();
        this.lat = previous.getLat();
        this.id = previous.getId();
        this.arrivalTime = previous.getArrivalTime();
    }

    public String getName() {
        return name;
    }

    public BigDecimal getLon() {
        return lon;
    }

    public BigDecimal getLat() {
        return lat;
    }

    public int getId() {
        return id;
    }

    public String getArrivalTime() {
        return arrivalTime;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setLon(BigDecimal lon) {
        this.lon = lon;
    }

    public void setLat(BigDecimal lat) {
        this.lat = lat;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setArrivalTime(String arrivalTime) {
        this.arrivalTime = arrivalTime;
    }

    /**
     * Add a stop to the database
     *
     * @param distantCo connection to the MINT database
     * @throws SQLException Error with SQL connection
     */
    public void addStopVertices(ConnectionDB distantCo) throws SQLException {
        String the_geom;
        PreparedStatement stmt;

        String query = "INSERT INTO ways_vertices_pgr(tan_data, station_name, lat, lon, the_geom) VALUES (true, ?, ?, ?, ST_GeomFromText(?)) RETURNING ID";  // insert stops data into MINT server

        the_geom = "POINT(" + this.getLon() + " " + this.getLat() + ")";

        stmt = distantCo.getConnect().prepareStatement(query);
        stmt.setString(1, this.getName());
        stmt.setBigDecimal(2, this.getLat());  // adapt for numeric type
        stmt.setBigDecimal(3, this.getLon());
        stmt.setString(4, the_geom);

        ResultSet idGenerated = stmt.executeQuery();  // get the id of the inserted data
        idGenerated.next();
        this.setId(idGenerated.getInt("id"));
    }

    /**
     * Link a TAN stop with the 5 closest nodes from OSM
     *
     * @param distantCo connection with mint application database
     * @throws SQLException Problem with SQL statement
     */
    public void linkWithPedestrianGraph(ConnectionDB distantCo) throws SQLException {
        // Get a list of close nodes that are not from tan_data : access bus/tram stops as a pedestrian
        String query1 = "SELECT id, lon, lat FROM ways_vertices_pgr WHERE tan_data=false ORDER BY the_geom <-> ST_SetSRID(ST_Point (?, ?),4326) limit 5;";
        PreparedStatement stmt1 = distantCo.getConnect().prepareStatement(query1);
        stmt1.setBigDecimal(1, this.getLon());
        stmt1.setBigDecimal(2, this.getLat());
        ResultSet pedestrianNodes = stmt1.executeQuery();

        Way wayAdded = new Way(this);

        while (pedestrianNodes.next()) {
            wayAdded.setCurrent(new Stop());
            wayAdded.getCurrent().setId(pedestrianNodes.getInt("id"));
            wayAdded.getCurrent().setLon(pedestrianNodes.getBigDecimal("lon"));
            wayAdded.getCurrent().setLat(pedestrianNodes.getBigDecimal("lat"));

            if (wayAdded.distanceLonLat() < 200) {  // don't add a link with a faraway node
                wayAdded.insertAWayWithPedestrian(distantCo);
            }
        }
    }


    /**
     * Get lon, lat and id from MINT database using the station_name
     *
     * @param distantCo     connection to MINT database
     * @throws SQLException sql error
     */
    public void fetchFromStationName(ConnectionDB distantCo) throws SQLException {
        // get data for current stop
        String query = "SELECT id, lon, lat FROM ways_vertices_pgr WHERE station_name = ? AND tan_data";
        PreparedStatement stmt = distantCo.getConnect().prepareStatement(query);

        // Current stop (target) query
        stmt.setString(1, this.getName());
        ResultSet res = stmt.executeQuery();
        res.next();

        // Change stop data
        this.setId(res.getInt("id"));
        this.setLon(res.getBigDecimal("lon"));
        this.setLat(res.getBigDecimal("lat"));
    }
}
