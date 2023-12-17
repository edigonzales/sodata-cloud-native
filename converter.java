///usr/bin/env jbang "$0" "$@" ; exit $?
//REPOS mavencentral
//REPOS ehi=https://jars.interlis.ch/
//DEPS io.github.sogis:meta2file:1.0.71 com.fasterxml.jackson.core:jackson-core:2.16.0 com.fasterxml.jackson.core:jackson-annotations:2.16.0 com.fasterxml.jackson.core:jackson-databind:2.16.0 com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.16.0 com.fasterxml.jackson.dataformat:jackson-dataformat-xml:2.16.0 net.lingala.zip4j:zip4j:2.11.4 org.xerial:sqlite-jdbc:3.40.1.0 ch.interlis:iox-ili:1.22.0 ch.interlis:ili2c-core:5.3.3 ch.interlis:ili2c-tool:5.3.3

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
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.xml.stream.XMLStreamException;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import ch.interlis.ili2c.Ili2c;
import ch.interlis.ili2c.Ili2cException;
import ch.interlis.ili2c.config.Configuration;
import ch.interlis.ili2c.metamodel.TransferDescription;
import ch.interlis.ilirepository.IliManager;
import ch.interlis.iom.IomObject;
import ch.interlis.iom_j.Iom_jObject;
import ch.interlis.iom_j.xtf.XtfReader;
import ch.interlis.iom_j.xtf.XtfWriter;
import ch.interlis.iox.IoxEvent;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.IoxReader;
import ch.interlis.iox.IoxWriter;
import ch.interlis.iox_j.EndBasketEvent;
import ch.interlis.iox_j.EndTransferEvent;
import ch.interlis.iox_j.ObjectEvent;
import ch.interlis.iox_j.StartBasketEvent;
import ch.interlis.iox_j.StartTransferEvent;
import ch.so.agi.meta2file.model.FileFormat;
import ch.so.agi.meta2file.model.Item;
import ch.so.agi.meta2file.model.Office;
import ch.so.agi.meta2file.model.ThemePublication;

public class converter {

    private static final String DATASEARCH_XML_NAME = "datasearch.xml";
    private static final String DATASEARCH_XTF_NAME = "datasearch.xtf";
    private static final String DATASEARCH_XML_URL = "https://s3.eu-central-1.amazonaws.com/ch.so.agi.sodata-prod/" + DATASEARCH_XML_NAME;
    private static final String DATASEARCH_XTF_URL = "https://s3.eu-central-1.amazonaws.com/ch.so.agi.sodata-prod/" + DATASEARCH_XTF_NAME;

    private static final String FILES_SERVER_URL = "https://files.geo.so.ch";

    private static final String WORK_DIR = System.getProperty("java.io.tmpdir");
    private static final String DATA_DIR = System.getenv("DATA_DIR")!=null ? System.getenv("DATA_DIR") : "/data"; 

    private static final String ILI_MODEL_NAME = "SO_AGI_STAC_20230426";
    private static final String ILI_TOPIC = ILI_MODEL_NAME+".Collections";
    private static final String BID = ILI_MODEL_NAME+".Collections";
    private static final String TAG = ILI_MODEL_NAME+".Collections.Collection";
    //private static final String CLASS_DESCRIPTION_TAG = ILI_MODEL_NAME+".ClassDescription";
    //private static final String ATTRIBUTE_DESCRIPTION_TAG = ILI_MODEL_NAME+".AttributeDescription";
    private static final String OFFICE_STRUCTURE_TAG = ILI_MODEL_NAME+"."+ILI_TOPIC+".Office";
    //private static final String MODELLINK_STRUCTURE_TAG = ILI_MODEL_NAME+".ModelLink";
    private static final String BOUNDARY_STRUCTURE_TAG = ILI_MODEL_NAME+"."+ILI_TOPIC+".BoundingBox";
    private static final String INTERVAL_STRUCTURE_TAG = ILI_MODEL_NAME+"."+ILI_TOPIC+".Interval";
    private static final String KEYWORD_STRUCTURE_TAG = ILI_MODEL_NAME+"."+ILI_TOPIC+".Keyword_";
    private static final String ITEM_STRUCTURE_TAG = ILI_MODEL_NAME+"."+ILI_TOPIC+".Item";
    private static final String ASSET_STRUCTURE_TAG = ILI_MODEL_NAME+"."+ILI_TOPIC+".Asset";
    //private static final String FILEFORMAT_STRUCTURE_TAG = ILI_MODEL_NAME+".FileFormat";

    private static HttpClient httpClient;

    private static IoxReader ioxReader;
    private static IoxWriter ioxWriter;

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    public static void main(String... args) throws IOException, InterruptedException, URISyntaxException, XMLStreamException, Ili2cException, IoxException, SQLException {
        // Http client
        httpClient = HttpClient.newBuilder()
            .version(Version.HTTP_1_1)
            .build();

        // Create meta xtf
        TransferDescription td = getTransferDescriptionFromModelName(ILI_MODEL_NAME);
        File xtfFile = Paths.get(DATA_DIR, "meta.xtf").toFile();
        ioxWriter = new XtfWriter(xtfFile, td);

        ioxWriter.write(new StartTransferEvent("SOGIS-20231217", "", null));
        ioxWriter.write(new StartBasketEvent(ILI_TOPIC,BID));

        // Download original meta xtf
        var datasearchXtfFile = new File(DATASEARCH_XTF_NAME);       
        // var httpRequest = HttpRequest.newBuilder().GET().uri(new URI(DATASEARCH_XTF_URL))
        //        .timeout(Duration.ofSeconds(30L)).build();
        // var response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofInputStream());
        // saveFile(response.body(), datasearchXtfFile.getAbsolutePath());

        ioxReader = new XtfReader(datasearchXtfFile);
        IoxEvent event = ioxReader.read();
        while (event != null) {
            
            if (event instanceof StartBasketEvent) {
                StartBasketEvent basket = (StartBasketEvent) event;
                //logger.debug(basket.getType()+"(oid "+basket.getBid()+")...");

            } else if (event instanceof ObjectEvent) {
                IomObject iomObj = ((ObjectEvent)event).getIomObject();
                String tag = iomObj.getobjecttag();

                // Raster werden ignoriert
                if (iomObj.getattrobj("Items", 0).getattrobj("Assets", 0).getattrvalue("MediaType").contains("tiff")) {
                    event = ioxReader.read();
                    continue;
                }

                boolean gpkg = false;
                IomObject item = iomObj.getattrobj("Items", 0);
                for (int i=0; i<item.getattrvaluecount("Assets"); i++) {
                    IomObject asset = item.getattrobj("Assets", i);
                    if (asset.getattrvalue("MediaType").contains("geopackage")) {
                        gpkg = true;
                    }
                }
                if (!gpkg) {
                    event = ioxReader.read();
                    continue;
                }

                String identifier = iomObj.getattrvalue("Identifier");
                err.println(iomObj.getattrvalue("Identifier"));
                //ioxWriter.write(new ObjectEvent(iomObj));

                // TODO REMOVE
                if (iomObj.getattrvalue("Identifier").contains("hoehenlinien")) {
                    event = ioxReader.read();          
                    continue;
                }

                // TODO
                // Ich glaub die Unterwcheidung muss in die convertDataset-Methode
                // Das Item darf ich erst nach durchlaufen aller Items ändern.
                // deleteObjs und dann addObjs
                if (iomObj.getattrvaluecount("Items") > 1) {
                    for (int i=0; i<iomObj.getattrvaluecount("Items"); i++) {
                        IomObject itemObj = iomObj.getattrobj("Items", i);
                        String itemIdentifier = itemObj.getattrvalue("Identifier");
                        err.println("----------------------------------------------------");      
                        err.println("Converting dataset: " + itemIdentifier); 
                        IomObject foo = convertDataset(itemIdentifier, iomObj, itemObj); 
                        //iomObj.addattrobj("Items", foo);                 
                    }
                } else {
                    err.println("----------------------------------------------------");      
                    err.println("Converting dataset: " + identifier);                   
                    IomObject foo = convertDataset(identifier, iomObj, iomObj.getattrobj("Items", 0));    
                    //iomObj.addattrobj("Items", foo);                 
              
                }

                ioxWriter.write(new ObjectEvent(iomObj));


            } else if (event instanceof EndBasketEvent) {

            }
            else if (event instanceof EndTransferEvent) {
                ioxReader.close();                
                break;
            }
            event = ioxReader.read();
        }                                               

        ioxWriter.write(new EndBasketEvent());
        ioxWriter.write(new EndTransferEvent());
        ioxWriter.flush();
        ioxWriter.close();    



        // TransferDescription td = getTransferDescriptionFromModelName(ILI_MODEL_NAME);
        // File xtfFile = Paths.get(DATA_DIR, "meta.xtf").toFile();
        // ioxWriter = new XtfWriter(xtfFile, td);

        // ioxWriter.write(new StartTransferEvent("SOGIS-20231217", "", null));
        // ioxWriter.write(new StartBasketEvent(ILI_TOPIC,BID));

        // // Download metadata xml file from s3
        // var datasearchXmlFile = new File(DATASEARCH_XML_NAME);       
        // httpRequest = HttpRequest.newBuilder().GET().uri(new URI(DATASEARCH_XML_URL))
        //        .timeout(Duration.ofSeconds(30L)).build();
        // response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofInputStream());
        // saveFile(response.body(), datasearchXmlFile.getAbsolutePath());

        // // XML parsen und Datensatz konvertieren
        // var xmlMapper = new XmlMapper();
        // xmlMapper.registerModule(new JavaTimeModule());
        // xmlMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        // var xif = XMLInputFactory.newInstance();
        // var xr = xif.createXMLStreamReader(new FileInputStream(datasearchXmlFile));

        // while (xr.hasNext()) {
        //     xr.next();
        //     if (xr.getEventType() == XMLStreamConstants.START_ELEMENT) {
        //         if ("themePublication".equals(xr.getLocalName())) {
        //             var themePublication = xmlMapper.readValue(xr, ThemePublication.class);
        //             var identifier = themePublication.getIdentifier();
        //             var items = themePublication.getItems();
                               
        //             // Rasterdaten werden nicht konvertiert.
        //             if (themePublication.getModel() == null || themePublication.getModel().equalsIgnoreCase("")) {
        //                 continue; // ignore raster data
        //             }
                    
        //             // Falls kein GeoPackage vorhanden, wird der Datensatz nicht konvertiert.
        //             boolean gpkg = false;
        //             for (FileFormat fileFormat : themePublication.getFileFormats()) {
        //                 if (fileFormat.getAbbreviation().contains("gpkg")) gpkg = true;
        //             }
        //             if (!gpkg) continue;

        //             try {
        //                 if (items.size() > 1) {
        //                     for (Item item : items) {
        //                         var qualifiedIdentifier = item.getIdentifier() + "." + identifier;
        //                         err.println("----------------------------------------------------");      
        //                         err.println("Converting dataset: " + qualifiedIdentifier);
        //                         //convertDataset(qualifiedIdentifier, themePublication);
        //                     }
        //                 } else {      
        //                     err.println("----------------------------------------------------");      
        //                     err.println("Converting dataset: " + identifier);                   
        //                     convertDataset(identifier, themePublication);
        //                     //break;
        //                 }
        //            } catch (URISyntaxException | IOException | InterruptedException | SQLException e) {
        //                 e.printStackTrace();
        //            }
        //         }
        //     }
        // }

        // ioxWriter.write(new EndBasketEvent());
        // ioxWriter.write(new EndTransferEvent());
        // ioxWriter.flush();
        // ioxWriter.close();    

        out.println("Hello World");
    }

    private static IomObject convertDataset(String identifier, IomObject iomObj, IomObject item) throws URISyntaxException, IOException, InterruptedException, SQLException, IoxException {
        boolean hasSubunits = iomObj.getattrvaluecount("Items") > 1 ? true : false;

        // Verzeichnisse erstellen, falls nicht vorhanden
        File resultRootDir;
        if (hasSubunits) {
            resultRootDir = Paths.get(DATA_DIR, iomObj.getattrvalue("Identifier"), identifier.substring(0, identifier.indexOf("."))).toFile();
        } else {
            resultRootDir = Paths.get(DATA_DIR, identifier).toFile();
        }

        if (!resultRootDir.exists()) resultRootDir.mkdirs();
        err.println("Result root directory: " + resultRootDir);

        Map<String,String> formats = Map.of("Parquet", "parquet", "FlatGeobuf", "fgb");
        for (var format : formats.entrySet()) {
            File formatDir = Paths.get(resultRootDir.getAbsolutePath(), format.getKey().toLowerCase()).toFile();
            if (!formatDir.exists()) formatDir.mkdirs(); 
        }

        // Herunterladen
        String requestUrl = null;
        for (int i=0; i<item.getattrvaluecount("Assets"); i++) {
            IomObject asset = item.getattrobj("Assets", i);
            err.println("***** " + asset.getattrvalue("MediaType"));
            if (asset.getattrvalue("MediaType").contains("geopackage")) {
                requestUrl = asset.getattrvalue("Href");
            }
        }
        err.println("Downloading: " + requestUrl);

        var zipFile = Paths.get(WORK_DIR, identifier + ".gpkg.zip").toFile();
        
        var httpRequest = HttpRequest.newBuilder().GET().uri(new URI(requestUrl))
                .timeout(Duration.ofSeconds(30L)).build();
        var response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofInputStream());
        saveFile(response.body(), zipFile.getAbsolutePath());

        // Entzippen
        try {
            err.println("Unzipping: " + zipFile);
            new ZipFile(zipFile).extractAll(WORK_DIR);
        } catch (ZipException e) {
            throw new IOException(e);
        } 

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


        IomObject assets = new Iom_jObject(ASSET_STRUCTURE_TAG, null);
        // item.addattrobj("Assets", assets);
        // iomObj.addattrobj("Items", item);
        

        return item;

        //ioxWriter.write(new ObjectEvent(iomObj));


    }


    private static void convertDataset2(String identifier, ThemePublication themePublication) throws URISyntaxException, IOException, InterruptedException, SQLException, IoxException {
        boolean subunits = themePublication.getItems().size() > 1 ? true : false;
        
        // Verzeichnisse erstellen, falls nicht vorhanden
        File resultRootDir;
        if (subunits) {
            resultRootDir = Paths.get(DATA_DIR, themePublication.getIdentifier(), identifier.substring(0, identifier.indexOf("."))).toFile();
        } else {
            resultRootDir = Paths.get(DATA_DIR, identifier).toFile();
        }

        if (!resultRootDir.exists()) resultRootDir.mkdirs();
        //err.println("Result root directory: " + resultRootDir);

        Map<String,String> formats = Map.of("Parquet", "parquet", "FlatGeobuf", "fgb");
        for (var format : formats.entrySet()) {
            File formatDir = Paths.get(resultRootDir.getAbsolutePath(), format.getKey().toLowerCase()).toFile();
            if (!formatDir.exists()) formatDir.mkdirs(); 
        }
    
        // // Herunterladen
        // String requestUrl;
        // if (subunits) {
        //     requestUrl = FILES_SERVER_URL + "/" + themePublication.getIdentifier() + "/aktuell/" + identifier + ".gpkg.zip";            
        // } else  {
        //     requestUrl = FILES_SERVER_URL + "/" + identifier + "/aktuell/" + identifier + ".gpkg.zip";                        
        // }
        // err.println("Downloading: " + requestUrl);

        // var zipFile = Paths.get(WORK_DIR, identifier + ".gpkg.zip").toFile();
        
        // var httpRequest = HttpRequest.newBuilder().GET().uri(new URI(requestUrl))
        //         .timeout(Duration.ofSeconds(30L)).build();
        // var response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofInputStream());
        // saveFile(response.body(), zipFile.getAbsolutePath());

        // // Entzippen
        // try {
        //     err.println("Unzipping: " + zipFile);
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

                // try {
                //     ProcessBuilder pb = new ProcessBuilder(cmd.split(" "));  

                //     Process p = pb.start();
                //     {
                //         BufferedReader is = new BufferedReader(new InputStreamReader(p.getInputStream()));
                //         String line = null;
                //         while ((line = is.readLine()) != null)
                //             err.println(line);
                //         p.waitFor();
                //     }
                    
                //     if (p.exitValue() != 0) {
                //         err.println("Error: ogr2ogr did not run successfully: " + tableName + " - " + format.getKey() + " - " + cmd);
                //         err.println("Retry...");

                //         p = pb.start();
                //         BufferedReader is = new BufferedReader(new InputStreamReader(p.getInputStream()));
                //         String line = null;
                //         while ((line = is.readLine()) != null)
                //             err.println(line);
                //         p.waitFor();

                //         if (p.exitValue() != 0) {
                //             err.println("Failed again.");
                //         }
                //         //continue;
                //     }                
                // } catch (IOException | InterruptedException e) {
                //     e.printStackTrace();
                //     err.println(e.getMessage());
                //     return;
                // }
            }
        }

        //ioxWriter.write(new ObjectEvent(iomObj));

        // zipFile.delete();
        // gpkgFile.delete();
    }

    private static void saveFile(InputStream body, String destinationFile) throws IOException {
        var fos = new FileOutputStream(destinationFile);
        fos.write(body.readAllBytes());
        fos.close();
    }

    private static TransferDescription getTransferDescriptionFromModelName(String iliModelName) throws Ili2cException {
        IliManager manager = new IliManager();
        String repositories[] = new String[] { "https://geo.so.ch/models/" };
        manager.setRepositories(repositories);
        ArrayList<String> modelNames = new ArrayList<String>();
        modelNames.add(iliModelName);
        Configuration config = manager.getConfig(modelNames, 2.3);
        TransferDescription iliTd = Ili2c.runCompiler(config);

        if (iliTd == null) {
            throw new IllegalArgumentException("INTERLIS compiler failed"); // TODO: can this be tested?
        }
        
        return iliTd;
    }

    // private static int getTid() {
    //     return tid++;
    // }

    private static IomObject office2Iom(Office office) {
       // office.get


        return null;
    }

}
