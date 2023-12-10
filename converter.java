///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS io.github.sogis:meta2file:1.0.71 com.fasterxml.jackson.core:jackson-core:2.16.0 com.fasterxml.jackson.core:jackson-annotations:2.16.0 com.fasterxml.jackson.core:jackson-databind:2.16.0 com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.16.0 com.fasterxml.jackson.dataformat:jackson-dataformat-xml:2.16.0 net.lingala.zip4j:zip4j:2.11.4 org.xerial:sqlite-jdbc:3.40.1.0 

import static java.lang.System.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.exception.ZipException;

import ch.so.agi.meta2file.model.FileFormat;
import ch.so.agi.meta2file.model.Item;
import ch.so.agi.meta2file.model.ThemePublication;

public class converter {

    private static final String DATASEARCH_XML_NAME = "datasearch.xml";
    private static final String DATASEARCH_XML_URL = "https://s3.eu-central-1.amazonaws.com/ch.so.agi.sodata-prod/" + DATASEARCH_XML_NAME;

    private static final String FILES_SERVER_URL = "https://files.geo.so.ch";

    private static final String WORK_DIR = System.getProperty("java.io.tmpdir");
    private static final String DATA_DIR = System.getenv("DATA_DIR")!=null ? System.getenv("DATA_DIR") : "/Users/stefan/tmp/cloud-native/"; 

    private static HttpClient httpClient;

    public static void main(String... args) throws IOException, InterruptedException, URISyntaxException, XMLStreamException {

        httpClient = HttpClient.newBuilder()
            .version(Version.HTTP_1_1)
            .build();

        var datasearchXmlFile = new File(DATASEARCH_XML_NAME);
        
        // Download metadata xml file from s3
        // var httpRequest = HttpRequest.newBuilder().GET().uri(new URI(DATASEARCH_XML_URL))
        //        .timeout(Duration.ofSeconds(30L)).build();
        // var response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofInputStream());
        // saveFile(response.body(), datasearchXmlFile.getAbsolutePath());

        // XML parsen und Datensatz konvertieren
        var xmlMapper = new XmlMapper();
        xmlMapper.registerModule(new JavaTimeModule());
        xmlMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        var xif = XMLInputFactory.newInstance();
        var xr = xif.createXMLStreamReader(new FileInputStream(datasearchXmlFile));

        while (xr.hasNext()) {
            xr.next();
            if (xr.getEventType() == XMLStreamConstants.START_ELEMENT) {
                if ("themePublication".equals(xr.getLocalName())) {
                    var themePublication = xmlMapper.readValue(xr, ThemePublication.class);
                    var identifier = themePublication.getIdentifier();
                    var items = themePublication.getItems();
                               
                    // Rasterdaten werden nicht konvertiert.
                    if (themePublication.getModel() == null || themePublication.getModel().equalsIgnoreCase("")) {
                        continue; // ignore raster data
                    }
                    
                    // Falls kein GeoPackage vorhanden, wird der Datensatz nicht konvertiert.
                    boolean gpkg = false;
                    for (FileFormat fileFormat : themePublication.getFileFormats()) {
                        if (fileFormat.getAbbreviation().contains("gpkg")) gpkg = true;
                    }
                    if (!gpkg) continue;

                    try {
                        if (items.size() > 1) {
                            for (Item item : items) {
                                var qualifiedIdentifier = item.getIdentifier() + "." + identifier;
                                err.println("----------------------------------------------------");      
                                err.println("Converting dataset: " + qualifiedIdentifier);
                                convertDataset(qualifiedIdentifier, themePublication);
                            }
                        } else {      
                            err.println("----------------------------------------------------");      
                            err.println("Converting dataset: " + identifier);                   
                            convertDataset(identifier, themePublication);
                            //break;
                        }
                   } catch (URISyntaxException | IOException | InterruptedException | SQLException e) {
                        e.printStackTrace();
                   }
                }
            }
        }        



        out.println("Hello World");
    }

    private static void convertDataset(String identifier, ThemePublication themePublication) throws URISyntaxException, IOException, InterruptedException, SQLException {
        boolean subunits = themePublication.getItems().size() > 1 ? true : false;
        
        Map<String,String> formats = Map.of("Parquet", "parquet", "FlatGeobuf", "fgb");

        // Verzeichnisse erstellen, falls nicht vorhanden
        File resultRootDir;
        if (subunits) {
            resultRootDir = Paths.get(DATA_DIR, themePublication.getIdentifier(), identifier.substring(0, identifier.indexOf("."))).toFile();
        } else {
            resultRootDir = Paths.get(DATA_DIR, identifier).toFile();
        }

        if (!resultRootDir.exists()) resultRootDir.mkdirs();

        for (var format : formats.entrySet()) {
            File formatDir = Paths.get(resultRootDir.getAbsolutePath(), format.getKey().toLowerCase()).toFile();
            if (!formatDir.exists()) formatDir.mkdirs(); 
        }
    

        //err.println("Result root directory: " + resultRootDir);

        // TODO:
        // - Nach subunit-name noch das Format als Unterordner.


        // Herunterladen
        // String requestUrl;
        // if (subunits) {
        //     requestUrl = FILES_SERVER_URL + "/" + themePublication.getIdentifier() + "/aktuell/" + identifier + ".gpkg.zip";            
        // } else  {
        //     requestUrl = FILES_SERVER_URL + "/" + identifier + "/aktuell/" + identifier + ".gpkg.zip";                        
        // }
        
        // var zipFile = Paths.get(WORK_DIR, identifier + ".gpkg.zip").toFile();
        
        // var httpRequest = HttpRequest.newBuilder().GET().uri(new URI(requestUrl))
        //         .timeout(Duration.ofSeconds(30L)).build();
        // var response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofInputStream());
        // saveFile(response.body(), zipFile.getAbsolutePath());

        // Entzippen
        // try {
        //     new ZipFile(zipFile).extractAll(WORK_DIR);
        // } catch (ZipException e) {
        //     throw new IOException(e);
        // } 

        // Alle Tabellen eruieren, die konvertiert werden.
        var gpkgFile = Paths.get(WORK_DIR, identifier + ".gpkg").toFile();
        var tableNames = new ArrayList<String>();
        var url = "jdbc:sqlite:" + gpkgFile;
        try (var conn = DriverManager.getConnection(url)) {
            try (var stmt = conn.createStatement()) {
                var rs = stmt.executeQuery("SELECT tablename FROM T_ILI2DB_TABLE_PROP WHERE setting = 'CLASS'"); 
                while (rs.next()) {
                    tableNames.add(rs.getString("tablename"));
                }
            }
        } 

        // Konvertieren
        for (String tableName : tableNames) {
            err.println("Converting table: " + tableName);

            for (var format : formats.entrySet()) {
                var outputFileName = tableName + "." + format.getValue();
                var outputDir = Paths.get(resultRootDir.getAbsolutePath(), format.getKey().toLowerCase()).toFile().getAbsolutePath();

                var lco = "";
                if (format.getValue().equals("fgb")) {
                    // Whitespace zu Beginn, dafür bei cmd nicht. Wegen cmd.split(" "). Führt zu fehlerhaften Befehl für ProcessBuilder.
                    lco = " -lco SPATIAL_INDEX=YES -lco TEMPORARY_DIR=/tmp";
                }

                var cmd = "docker run --rm -v " + WORK_DIR + ":/tmp -v " + outputDir + ":/data ghcr.io/osgeo/gdal:ubuntu-full-latest ogr2ogr" + lco + " -f " + format.getKey() + " /data/" + outputFileName + " /tmp/" + gpkgFile.getName() + " " + tableName;
                //err.println(cmd);

                try {
                    ProcessBuilder pb = new ProcessBuilder(cmd.split(" "));  

                    Process p = pb.start();
                    {
                        BufferedReader is = new BufferedReader(new InputStreamReader(p.getInputStream()));
                        String line = null;
                        while ((line = is.readLine()) != null)
                            err.println(line);
                        p.waitFor();
                    }
                    
                    if (p.exitValue() != 0) {
                        err.println("Error: ogr2ogr did not run successfully: " + tableName + " - " + format.getKey() + " - " + cmd);
                        err.println("Retry...");

                        p = pb.start();
                        BufferedReader is = new BufferedReader(new InputStreamReader(p.getInputStream()));
                        String line = null;
                        while ((line = is.readLine()) != null)
                            err.println(line);
                        p.waitFor();

                        if (p.exitValue() != 0) {
                            err.println("Failed again.");
                        }

                        continue;
                    }
                    
                    // String location;
                    // if (subunits) {
                    //     location = themePublication.getIdentifier() + "/" + identifier;
                    // } else {
                    //     location = identifier;
                    // }
                    //amazonS3StorageService.store(outputFile, outputFile.getName(), location);
                
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                    err.println(e.getMessage());
                    return;
                }
            }
        }
    }

    private static void saveFile(InputStream body, String destinationFile) throws IOException {
        var fos = new FileOutputStream(destinationFile);
        fos.write(body.readAllBytes());
        fos.close();
    }
}
