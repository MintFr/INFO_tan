import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MainTries
{
    public static void main(String[] args) throws SQLException {
        ConnectionDB distantCO = new ConnectionDB("admin", "postgres", "routing_pedestrian_test");
        String query = "INSERT INTO ways_vertices_pgr(tan_data, station_name, lat, lon, the_geom) VALUES\n" +
                "(true, ?, ?, ?, ST_GeomFromText(?))";
        PreparedStatement stmt1 = distantCO.getConnect().prepareStatement(query);
        stmt1.setString(1, "ADRBknnjkn1");
        BigDecimal bigDecimal = new BigDecimal("47.2515015");
        stmt1.setBigDecimal(2, bigDecimal);
        bigDecimal = new BigDecimal("-5.5964529");
        stmt1.setBigDecimal(3, bigDecimal);
        stmt1.setString(4, "POINT(-5.285284 47.2515015)");
        ResultSet res1 = stmt1.executeQuery();
    }
}
