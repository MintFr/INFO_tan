package org.infopgrou;



import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;


/**
 * Data loader : Used to get the right updated url to download the gtfs data
 */
public class DataLoader {

    /**
     * The data ID API
     */
    private String api_url;

    /**
     * DataLoader constructor
     * @param api_url
     */
    public DataLoader(String api_url) {
        this.api_url = api_url;
    }


    /**
     * Getting the json data that includes the id as a Json Object
     * @param url the static url for the tan API
     * @return JSONObject contains the data API Id.
     */
    public JSONObject getJson(String url, Log logger) throws IOException, JSONException {
        InputStream api = new URL(url).openStream();
        try {

            BufferedReader reader = new BufferedReader(new InputStreamReader(api, Charset.forName("UTF-8")));
            String jsonText = "";
            String line = reader.readLine();
            while (line != null) {
                jsonText = jsonText + line;
                line = reader.readLine();
            }
            JSONObject json = new JSONObject(jsonText);
            logger.getLogger().info("JSON for the dataset id is OK");
            return json;

        } catch (MalformedURLException | JSONException e) {
            logger.getLogger().warning("Wrong url format" + e.getMessage());
            return new JSONObject("");
        } finally {
            api.close();
        }
    }

    /**
     * Construct the final Url for downloading the right updated gtfs zip file
     * @return l'url final
     */
    public String getUrl(Log logger) throws JSONException, IOException {
        JSONObject json = getJson(api_url, logger);
        JSONArray records = json.getJSONArray("records");
        String datasetid = records.getJSONObject(0).getString("datasetid");
        String id = records.getJSONObject(0).getJSONObject("fields").getJSONObject("fichier").getString("id");
        String res = "https://data.nantesmetropole.fr/explore/dataset/" + datasetid + "/files/" + id + "/download/";
        logger.getLogger().info("The updated url for gtfs.zip is ready : " + res);
        return res;
    }

    /**
     * Extract data from gtfs.zip to the extraction directory using the utility funtion @extractFile
     * @param zipDir : The zip directory from the download function
     * @param extractionDir : The extraction destination
     */
    public void unzipData(String zipDir, String extractionDir, Log logger) throws IOException {
        logger.getLogger().info("Start unzipping data from " + zipDir + " into " + extractionDir);
        File destDir = new File(extractionDir);
        if (!destDir.exists()) {
            destDir.mkdir();
        }
        ZipInputStream zipIn = new ZipInputStream(new FileInputStream(zipDir));
        ZipEntry zipEntry = zipIn.getNextEntry();

        while (zipEntry != null) {
            String filePath = extractionDir + File.separator + zipEntry.getName();
            if (!zipEntry.isDirectory()) {
                extractFile(zipIn, filePath, logger);
            } else {
                File dir = new File(filePath);
                dir.mkdir();
            }
            zipEntry = zipIn.getNextEntry();
        }
        logger.getLogger().info("Data is ready to use");
    }

    /**
     * utility function that takes a zip file an input stream of byes and put the result in the filePath
     * @param zipIn : the zip directory
     * @param filePath : destination path
     */
    public void extractFile(ZipInputStream zipIn, String filePath, Log logger) throws IOException {
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath));
        byte bytes[] = new byte[4096];
        int read = 0;
        read = zipIn.read(bytes);
        while (read != -1) {
            bos.write(bytes, 0, read);
            read = zipIn.read(bytes);
        }
        bos.close();
    }

    /**
     * Downloading the file from the constructed Url, and use the unzipping functions to put the text files in the data folder.
     * @param zipFilePath : the zip file path
     * @param destDirectory : destination directory path
     */
    public void downloadAndUnzip(String zipFilePath, String destDirectory, Log logger) throws IOException, JSONException {
        logger.getLogger().info("Downloading started ...");
        URL destURL = new URL(getUrl(logger));
        URLConnection urlConnection = destURL.openConnection();
        ReadableByteChannel zipByteChannel = Channels.newChannel(urlConnection.getInputStream());
        File destDir = new File(destDirectory);
        if (!destDir.exists()) {
            destDir.mkdir();
        }
        try (FileOutputStream fos = new FileOutputStream(zipFilePath)) {
            fos.getChannel().transferFrom(zipByteChannel, 0, Long.MAX_VALUE);
        }
        logger.getLogger().info("gtfs.zip downloaded to "+destDirectory + " successfully");
        unzipData(zipFilePath, destDirectory, logger);
    }
}

