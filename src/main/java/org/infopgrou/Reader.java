package org.infopgrou;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Scanner;
import java.util.StringTokenizer;

public class Reader {

    private BufferedReader reader;
    private String path;
    private String db_username;
    private String db_password;
    private String dbURL;
    private Log logger;

    public Reader(String path, String db_username, String db_password, String dbURL, Log logger) {
        this.path = path;
        this.db_username = db_username;
        this.db_password = db_password;
        this.dbURL = dbURL;
        this.logger = logger;
    }

    /**
     * Reading stops from stops.txt files
     */
    public void readStop(){
        logger.getLogger().info("Reading stops.txt ...");
        try {
            reader = new BufferedReader(new FileReader(path+"/stops.txt"));
        } catch (FileNotFoundException ex) {
            logger.getLogger().warning("File not found!");
        }
        try {
            String header = reader.readLine();
            String line = reader.readLine();
            String query = "TRUNCATE stops CASCADE";
            while (line != null) {

                StringTokenizer tokens = new StringTokenizer(line.replace(",", ", "), ",");

                String stop_id = tokens.nextToken().trim();
                String stop_name = tokens.nextToken().replace("'", "").trim();
                String stop_desc = tokens.nextToken().trim();
                String stop_lat = tokens.nextToken().trim();
                String stop_lon = tokens.nextToken().trim();
                String zone_id = tokens.nextToken().trim();
                String stop_url = tokens.nextToken().trim();
                String location_type = tokens.nextToken().trim();
                String parent_station = tokens.nextToken().trim();
                String wheelchair_boarding = tokens.nextToken().trim();

                query = query + ";INSERT INTO stops (stop_id,stop_name,stop_desc,stop_lat,stop_lon,zone_id,stop_url,location_type,parent_station,wheelchair_boarding)\n" +
                        "VALUES (" +
                        "'"+stop_id+"',"+
                        "'"+stop_name+"',"+
                        "'"+stop_desc+"',"+
                        "'"+stop_lat+"',"+
                        "'"+stop_lon+"',"+
                        "'"+zone_id+"',"+
                        "'"+stop_url+"',"+
                        "'"+location_type+"',"+
                        "'"+parent_station+"',"+
                        "'"+wheelchair_boarding+"'"+
                        ")";

                line = reader.readLine();
            }

            try {
                Class.forName("org.postgresql.Driver");

                Connection connect = DriverManager.getConnection(dbURL,db_username, db_password);

                PreparedStatement stmt = connect.prepareStatement(query);
                stmt.executeUpdate();

                stmt.close();
                connect.close();
            }
            catch(java.lang.ClassNotFoundException e)
            {
                logger.getLogger().warning("ClassNotFoundException : " + e.getMessage());
            }
            catch (SQLException ex)
            {
                logger.getLogger().warning("SQLException : " + ex.getMessage());
            }


        } catch(IOException ex) {
            logger.getLogger().warning("Can't read from file");
        }

        logger.getLogger().info("stops.txt saved with success!");
    }

    /**
     * Reading stop_times file
     */
    public void readStopTimes(){
        logger.getLogger().info("Reading stop_times.txt ...");

        InputStream is = null;


        try {
            Class.forName("org.postgresql.Driver");
            Connection connect = DriverManager.getConnection(dbURL,db_username, db_password);

            try {
                // Creating an instance of Inputstream
                is = new FileInputStream(path+"/stop_times.txt");
            } catch (FileNotFoundException ex) {
                logger.getLogger().warning("File not found!");
            }

            // Try block to check for exceptions
            try (Scanner sc = new Scanner(
                    is, StandardCharsets.UTF_8.name())) {

                // It holds true till there is single element
                // left in the object with usage of hasNext()
                // method
                String header = sc.nextLine();
                String query = "TRUNCATE stop_times CASCADE";
                PreparedStatement stmt = connect.prepareStatement(query + ";");
                stmt.executeUpdate();
                while (sc.hasNextLine()) {
                    String line = sc.nextLine();
                    // Printing the content of file

                    StringTokenizer tokens = new StringTokenizer(line.replace(",", ", "), ",");

                    String trip_id = tokens.nextToken().trim();
                    String arrival_time = tokens.nextToken().trim();
                    String departure_time = tokens.nextToken().trim();
                    String stop_id = tokens.nextToken().trim();
                    String stop_sequence = tokens.nextToken().trim();
                    String pickup_type = tokens.nextToken().trim();
                    String drop_off_type = tokens.nextToken().trim();
                    String timepoint = tokens.nextToken().trim();
                    String stop_headsign = tokens.nextToken().trim().replace("'", " ");

                    query = "INSERT INTO stop_times (trip_id,arrival_times,departure_time,stop_id,stop_sequence,pickup_type,drop_off_type,timepoint,stop_headsign)\n" +
                            "VALUES (" +
                            "'"+trip_id+"',"+
                            "'"+arrival_time+"',"+
                            "'"+departure_time+"',"+
                            "'"+stop_id+"',"+
                            "'"+stop_sequence+"',"+
                            "'"+pickup_type+"',"+
                            "'"+drop_off_type+"',"+
                            "'"+timepoint+"',"+
                            "'"+stop_headsign+"'"+
                            ")";
                    stmt = connect.prepareStatement(query + ";");
                    stmt.executeUpdate();
                }

                stmt.close();
                connect.close();
            } catch (SQLException ex)
            {
                logger.getLogger().warning("SQLException : " + ex.getMessage());
            }
        } catch (SQLException | ClassNotFoundException throwables) {
            throwables.printStackTrace();
        }


        logger.getLogger().info("stop_times.txt saved with success!");
    }

    /**
     * Reading Calendar Dates
     */
    public void readCalendarDates(){

        logger.getLogger().info("Reading calendar_dates.txt ...");
        try {
            reader = new BufferedReader(new FileReader(path+"/calendar_dates.txt"));
        } catch (FileNotFoundException ex) {
            logger.getLogger().warning("File not found!");
        }

        try {
            String header = reader.readLine();
            String line = reader.readLine();
            String query = "TRUNCATE calendar_dates CASCADE";
            while (line != null) {

                StringTokenizer tokens = new StringTokenizer(line.replace(",", ", "), ",");

                String service_id = tokens.nextToken().trim();
                String date = tokens.nextToken().trim();
                String exception_type = tokens.nextToken().trim();

                query = query + ";INSERT INTO calendar_dates (service_id,date,exception_type)\n" +
                        "VALUES (" +
                        "'"+service_id+"',"+
                        "'"+date+"',"+
                        "'"+exception_type+"'"+
                        ")";

                line = reader.readLine();
            }

            try {
                Class.forName("org.postgresql.Driver");
                Connection connect = DriverManager.getConnection(dbURL,db_username, db_password);

                PreparedStatement stmt = connect.prepareStatement(query + ";");
                stmt.executeUpdate();

                stmt.close();
                connect.close();
            }
            catch(java.lang.ClassNotFoundException e)
            {
                logger.getLogger().warning("ClassNotFoundException : " + e.getMessage());
            }
            catch (SQLException ex)
            {
                logger.getLogger().warning("SQLException : " + ex.getMessage());
            }
        } catch(IOException ex) {
            logger.getLogger().warning("Can't read from file");
        }

        logger.getLogger().info("calendar_dates.txt saved with success!");
    }

    /**
     * Reading Calendar
     */
    public void readCalendar(){

        logger.getLogger().info("Reading calendar.txt ...");
        try {
            reader = new BufferedReader(new FileReader(path + "/calendar.txt"));
        } catch (FileNotFoundException ex) {
            logger.getLogger().warning("File not found!");
        }

        try {
            String header = reader.readLine();
            String line = reader.readLine();
            String query = "TRUNCATE calendar CASCADE";
            while (line != null) {

                StringTokenizer tokens = new StringTokenizer(line.replace(",", ", "), ",");

                String service_id = tokens.nextToken().trim();
                String monday = tokens.nextToken().trim();
                String tuesday = tokens.nextToken().trim();
                String wednesday = tokens.nextToken().trim();
                String thursday = tokens.nextToken().trim();
                String friday = tokens.nextToken().trim();
                String saturday = tokens.nextToken().trim();
                String sunday = tokens.nextToken().trim();
                String start_date = tokens.nextToken().trim();
                String end_date = tokens.nextToken().trim();

                query = query + ";INSERT INTO calendar (service_id,start_date,end_date)\n" +
                        "VALUES (" +
                        "'"+service_id+"',"+
                        "'"+start_date+"',"+
                        "'"+end_date+"'"+
                        ")";

                line = reader.readLine();
            }

            try {
                Class.forName("org.postgresql.Driver");
                Connection connect = DriverManager.getConnection(dbURL,db_username, db_password);

                PreparedStatement stmt = connect.prepareStatement(query + ";");
                stmt.executeUpdate();

                stmt.close();
                connect.close();
            }
            catch(java.lang.ClassNotFoundException e)
            {
                logger.getLogger().warning("ClassNotFoundException : " + e.getMessage());
            }
            catch (SQLException ex)
            {
                logger.getLogger().warning("SQLException : " + ex.getMessage());
            }
        } catch(IOException ex) {
            logger.getLogger().warning("Can't read from file");
        }

        logger.getLogger().info("calendar.txt saved with success!");
    }

    /**
     * Reading Routes
     */
    public void readRoutes(){
        logger.getLogger().info("Reading routes.txt");
        try {
            reader = new BufferedReader(new FileReader(path+"/routes.txt"));
        } catch (FileNotFoundException ex) {
            logger.getLogger().warning("File not found!");
        }

        try {
            String header = reader.readLine();
            String line = reader.readLine();
            String query = "TRUNCATE routes CASCADE";
            while (line != null) {

                StringTokenizer tokens = new StringTokenizer(line.replace(",", ", "), ",");

                String route_id = tokens.nextToken().trim();
                String route_short_name = tokens.nextToken().trim();
                String route_long_name = tokens.nextToken().trim().replace("'", " ");
                String route_desc = tokens.nextToken().trim();
                String route_type = tokens.nextToken().trim();
                String route_color = tokens.nextToken().trim();
                String route_text_color = tokens.nextToken().trim();


                query = query + ";INSERT INTO routes (route_id,route_short_name,route_long_name,route_desc,route_type,route_color,route_text_color)\n" +
                        "VALUES (" +
                        "'"+route_id+"',"+
                        "'"+route_short_name+"',"+
                        "'"+route_long_name+"',"+
                        "'"+route_desc+"',"+
                        "'"+route_type+"',"+
                        "'"+route_color+"',"+
                        "'"+route_text_color+"'"+
                        ")";

                line = reader.readLine();
            }

            try {
                Class.forName("org.postgresql.Driver");
                Connection connect = DriverManager.getConnection(dbURL,db_username, db_password);

                PreparedStatement stmt = connect.prepareStatement(query + ";");
                stmt.executeUpdate();

                stmt.close();
                connect.close();
            }
            catch(java.lang.ClassNotFoundException e)
            {
                logger.getLogger().warning("ClassNotFoundException : " + e.getMessage());
            }
            catch (SQLException ex)
            {
                logger.getLogger().warning("SQLException : " + ex.getMessage());
            }
        } catch(IOException ex) {
            logger.getLogger().warning("Can't read from file");
        }
        logger.getLogger().info("routes.txt saved with success!");
    }

    /**
     * Reading Trips
     */
    public void readTrips(){
        logger.getLogger().info("Reading trips.txt ...");

        InputStream is = null;


        try {
            Class.forName("org.postgresql.Driver");
            Connection connect = DriverManager.getConnection(dbURL,db_username, db_password);

            try {
                // Creating an instance of Inputstream
                is = new FileInputStream(path+"/trips.txt");
            } catch (FileNotFoundException ex) {
                logger.getLogger().warning("File not found!");
            }

            // Try block to check for exceptions
            try (Scanner sc = new Scanner(
                    is, StandardCharsets.UTF_8.name())) {

                // It holds true till there is single element
                // left in the object with usage of hasNext()
                // method
                String header = sc.nextLine();
                String query = "TRUNCATE trip CASCADE";
                PreparedStatement stmt = connect.prepareStatement(query + ";");
                stmt.executeUpdate();
                while (sc.hasNextLine()) {
                    String line = sc.nextLine();
                    // Printing the content of file

                    StringTokenizer tokens = new StringTokenizer(line.replace(",", ", "), ",");

                    String route_id = tokens.nextToken().trim();
                    String service_id = tokens.nextToken().trim();
                    String trip_id = tokens.nextToken().trim();
                    String trip_headsign = tokens.nextToken().trim().replace("'", " ");
                    String direction_id = tokens.nextToken().trim();
                    String block_id = tokens.nextToken().trim();
                    String shape_id = tokens.nextToken().trim();
                    String wheelchair_accessible = tokens.nextToken().trim();


                    query = "INSERT INTO trip (route_id,service_id,trip_id,trip_headsign,direction_id,block_id,shape_id,wheelchair_accessible)\n" +
                            "VALUES (" +
                            "'"+route_id+"',"+
                            "'"+service_id+"',"+
                            "'"+trip_id+"',"+
                            "'"+trip_headsign+"',"+
                            "'"+direction_id+"',"+
                            "'"+block_id+"',"+
                            "'"+shape_id+"',"+
                            "'"+wheelchair_accessible+"'"+
                            ")";
                    stmt = connect.prepareStatement(query + ";");
                    stmt.executeUpdate();
                }

                stmt.close();
                connect.close();
            } catch (SQLException ex)
            {
                logger.getLogger().warning("SQLException : " + ex.getMessage());
            }
        } catch (SQLException | ClassNotFoundException throwables) {
            throwables.printStackTrace();
        }


        logger.getLogger().info("trips.txt saved with success!");

    }
}
