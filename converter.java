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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.time.Duration;

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

public class converter {

    private static final String DATASEARCH_XML_NAME = "datasearch.xml";
    private static final String DATASEARCH_XTF_NAME = "datasearch.xtf";
    private static final String DATASEARCH_XML_URL = "https://s3.eu-central-1.amazonaws.com/ch.so.agi.sodata-prod/" + DATASEARCH_XML_NAME;
    private static final String DATASEARCH_XTF_URL = "https://s3.eu-central-1.amazonaws.com/ch.so.agi.sodata-prod/" + DATASEARCH_XTF_NAME;

    private static final String FILES_SERVER_URL = "https://files.geo.so.ch";

    private static final String WORK_DIR = System.getProperty("java.io.tmpdir");
    private static final String DATA_DIR = System.getenv("DATA_DIR")!=null ? System.getenv("DATA_DIR") : "/mnt/HC_Volume_100196105/data"; 

    private static final Boolean CREATE_STATIC_DATASETS = System.getenv("CREATE_STATIC_DATASETS")!=null ? Boolean.valueOf(System.getenv("CREATE_STATIC_DATASETS") ) : false;

    private static final String ILI_MODEL_NAME = "SO_AGI_STAC_20230426";
    private static final String ILI_TOPIC = ILI_MODEL_NAME+".Collections";
    private static final String BID = ILI_MODEL_NAME+".Collections";
    private static final String ITEM_STRUCTURE_TAG = ILI_TOPIC+".Item";
    private static final String ASSET_STRUCTURE_TAG = ILI_TOPIC+".Asset";

    private static HttpClient httpClient;

    private static IoxReader ioxReader;
    private static IoxWriter ioxWriter;

    public static void main(String... args) throws IOException, InterruptedException, URISyntaxException, Ili2cException, IoxException, SQLException {
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
        var httpRequest = HttpRequest.newBuilder().GET().uri(new URI(DATASEARCH_XTF_URL))
               .timeout(Duration.ofSeconds(30L)).build();
        var response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofInputStream());
        saveFile(response.body(), datasearchXtfFile.getAbsolutePath());

        ioxReader = new XtfReader(datasearchXtfFile);
        IoxEvent event = ioxReader.read();
        while (event != null) {
            
            if (event instanceof StartBasketEvent) {
                StartBasketEvent basket = (StartBasketEvent) event;

            } else if (event instanceof ObjectEvent) {
                IomObject iomObj = ((ObjectEvent)event).getIomObject();

                // Raster werden ignoriert
                if (iomObj.getattrobj("Items", 0).getattrobj("Assets", 0).getattrvalue("MediaType").contains("tiff")) {

                    if (iomObj.getattrvalue("Identifier").contains("klima")) {
                        // do nothing 
                        // not cogtiff ready
                    } else {
                        //err.println(iomObj.getattrvalue("Identifier"));
                        
                        String identifier = iomObj.getattrvalue("Identifier");
                        IomObject boundary = iomObj.getattrobj("SpatialExtent", 0);
                        String westlimit = boundary.getattrvalue("westlimit");
                        String southlimit = boundary.getattrvalue("southlimit");
                        String eastlimit = boundary.getattrvalue("eastlimit");
                        String northlimit = boundary.getattrvalue("northlimit");
                        String geometry = "POLYGON(("+westlimit+" "+southlimit+", "+eastlimit +" "+ southlimit+", "+eastlimit +" "+ northlimit+", "+westlimit +" "+ northlimit+", "+westlimit +" "+ southlimit+"))";

                        IomObject itemObj = iomObj.getattrobj("Items", 0); // irgeneins ist iO.

                        IomObject newAssetObj = new Iom_jObject(ASSET_STRUCTURE_TAG, null);
                        newAssetObj.setattrvalue("Identifier", identifier + ".tif");
                        newAssetObj.setattrvalue("Title", iomObj.getattrvalue("Title"));
                        newAssetObj.setattrvalue("MediaType", "image/tiff; application=geotiff");
                        newAssetObj.setattrvalue("Href", "http://stac.sogeo.services/files/raster/" + identifier + ".tif");

                        IomObject newItemObj = new Iom_jObject(ITEM_STRUCTURE_TAG, null);
                        newItemObj.setattrvalue("Identifier", identifier);
                        newItemObj.setattrvalue("Title", iomObj.getattrvalue("Title"));
                        newItemObj.setattrvalue("Date", itemObj.getattrvalue("Date"));
                        newItemObj.addattrobj("Boundary", boundary);
                        newItemObj.setattrvalue("Geometry", geometry);
                        newItemObj.addattrobj("Assets", newAssetObj);

                        Iom_jObject newCollectionObj = new Iom_jObject(iomObj.getobjecttag(), iomObj.getobjectoid());
                        newCollectionObj.setattrvalue("Identifier", iomObj.getattrvalue("Identifier"));
                        newCollectionObj.setattrvalue("Title", iomObj.getattrvalue("Title"));
                        newCollectionObj.setattrvalue("ShortDescription", iomObj.getattrvalue("ShortDescription"));
                        newCollectionObj.setattrvalue("Licence", iomObj.getattrvalue("Licence"));
                        newCollectionObj.addattrobj("SpatialExtent", iomObj.getattrobj("SpatialExtent", 0));
                        newCollectionObj.addattrobj("TemporalExtent", iomObj.getattrobj("TemporalExtent", 0));
                        newCollectionObj.addattrobj("Owner", iomObj.getattrobj("Owner", 0));
                        newCollectionObj.addattrobj("Servicer", iomObj.getattrobj("Servicer", 0));
                        if (iomObj.getattrobj("Keywords", 0)!=null) {
                            newCollectionObj.addattrobj("Keywords", iomObj.getattrobj("Keywords", 0));
                        }
                        newCollectionObj.addattrobj("Items", newItemObj);
                        ioxWriter.write(new ObjectEvent(newCollectionObj));
                    }

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

                // Damit Tests/Develop effizienter ging.
                // Kann aber auch dafür verwendet werden, um die statischen 
                // Höhenlinien nur einmalig zu rechnen.
                if (!CREATE_STATIC_DATASETS) {
                    if (iomObj.getattrvalue("Identifier").contains("hoehenlinien")) {
                        event = ioxReader.read();          
                        continue;
                    }
                }

                // TODO REMOVE
                // if (!iomObj.getattrvalue("Identifier").contains("hoehenlinien")) {
                //         event = ioxReader.read();          
                //         continue;
                // }

                err.println("----------------------------------------------------");      
                err.println("Converting dataset: " + identifier);  
                try {
                    IomObject collectionObj = convertDataset(identifier, iomObj);  
                    if (collectionObj != null) {
                        ioxWriter.write(new ObjectEvent(collectionObj));
                    }        
                } catch (Exception e) {
                    err.println(e.getMessage());
                    event = ioxReader.read();
                    continue;
                }        
            } else if (event instanceof EndBasketEvent) {

            } else if (event instanceof EndTransferEvent) {
                ioxReader.close();                
                break;
            }
            event = ioxReader.read();
        }                                               

        ioxWriter.write(new EndBasketEvent());
        ioxWriter.write(new EndTransferEvent());
        ioxWriter.flush();
        ioxWriter.close();    

        out.println("Hello World");
    }

    private static IomObject convertDataset(String identifier, IomObject iomObj) throws URISyntaxException, IOException, InterruptedException, SQLException, IoxException {

        List<IomObject> newItemsObjList = new ArrayList<>();
        for (int i=0; i<iomObj.getattrvaluecount("Items"); i++) {
            String itemIdentifier = iomObj.getattrobj("Items", i).getattrvalue("Identifier");
            err.println("Converting item: " + itemIdentifier);

            // Verzeichnisse erstellen, falls nicht vorhanden
            File resultRootDir;
            File zipFile;
            File gpkgFile;
            if (identifier.equals(itemIdentifier)) {
                resultRootDir = Paths.get(DATA_DIR, identifier).toFile();
                zipFile = Paths.get(WORK_DIR, identifier + ".gpkg.zip").toFile();
                gpkgFile = Paths.get(WORK_DIR, identifier + ".gpkg").toFile();
            } else {
                resultRootDir = Paths.get(DATA_DIR, identifier, itemIdentifier.substring(0, itemIdentifier.indexOf("."))).toFile();
                zipFile = Paths.get(WORK_DIR, itemIdentifier + ".gpkg.zip").toFile();
                gpkgFile = Paths.get(WORK_DIR, itemIdentifier + ".gpkg").toFile();
            }

            if (!resultRootDir.exists()) resultRootDir.mkdirs();
            err.println("Result root directory: " + resultRootDir);

            Map<String,String> formats = Map.of("Parquet", "parquet", "FlatGeobuf", "fgb");
            for (var format : formats.entrySet()) {
                File formatDir = Paths.get(resultRootDir.getAbsolutePath(), format.getKey().toLowerCase()).toFile();
                if (!formatDir.exists()) formatDir.mkdirs(); 
            }

            // Herunterladen
            Iom_jObject itemObj = (Iom_jObject) iomObj.getattrobj("Items", i);
            String requestUrl = null;
            for (int ii=0; ii<itemObj.getattrvaluecount("Assets"); ii++) {
                IomObject asset = itemObj.getattrobj("Assets", ii);
                if (asset.getattrvalue("MediaType").contains("geopackage")) {
                    requestUrl = asset.getattrvalue("Href");
                }
            }
            err.println("Downloading: " + requestUrl);
    
            var httpRequest = HttpRequest.newBuilder().GET().uri(new URI(requestUrl))
                    .timeout(Duration.ofSeconds(30L)).build();
            var response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofInputStream());
            saveFile(response.body(), zipFile.getAbsolutePath());

            // Entzippen
            try {
                err.println("Unzipping: " + zipFile);
                String origFileName = gpkgFile.getName();
                if (identifier.contains("lidar_2014.hoehenlinien")) {
                    origFileName = itemIdentifier.substring(0,4) + itemIdentifier.substring(8, 12) + ".gpkg";
                    new ZipFile(zipFile).extractFile(origFileName, WORK_DIR, gpkgFile.getName());
                } else if (identifier.contains("lidar_2018.hoehenlinien") || identifier.contains("lidar_2019.hoehenlinien")) {
                    origFileName = itemIdentifier.substring(0, itemIdentifier.indexOf(".")) + ".gpkg";
                    new ZipFile(zipFile).extractFile(origFileName, WORK_DIR, gpkgFile.getName());
                } else {
                    new ZipFile(zipFile).extractAll(WORK_DIR);
                }

            } catch (ZipException e) {
                e.printStackTrace();
                err.println(e.getMessage());
                continue;
            } 

            // Alle Tabellen eruieren, die konvertiert werden.
            var tableNames = new ArrayList<String>();
            var url = "jdbc:sqlite:" + gpkgFile;
            try (var conn = DriverManager.getConnection(url)) {
                try (var stmt = conn.createStatement()) {
                    var rs = stmt.executeQuery("SELECT tablename FROM T_ILI2DB_TABLE_PROP WHERE setting = 'CLASS'"); 
                    while (rs.next()) {
                        tableNames.add(rs.getString("tablename"));
                    }
                } catch (Exception e) {
                    err.println(e.getMessage());
                    return null;
                }
            } catch (Exception e) {
                err.println(e.getMessage());
                return null;
            }

            // Aus STAC-Sicht mir nicht klar, ob korrekt. Oder ob es für jeden tableName 
            // eigentlich auch ein neues Item braucht.
            List<IomObject> newAssetsObjList = new ArrayList<>();
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
                            //continue;
                        }                
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                        err.println(e.getMessage());
                        return null;
                    }
                }

                {
                    IomObject newAssetObj = new Iom_jObject(ASSET_STRUCTURE_TAG, null);
                    newAssetObj.setattrvalue("Identifier", tableName + ".fgb");
                    newAssetObj.setattrvalue("Title", tableName + " (FlatGeobuf)");
                    newAssetObj.setattrvalue("MediaType", "application/flatgeobuf");
                    if (identifier.equals(itemIdentifier)) {
                        newAssetObj.setattrvalue("Href", "http://stac.sogeo.services/files/" + identifier + "/" + tableName + ".fgb");
                    } else {
                        newAssetObj.setattrvalue("Href", "http://stac.sogeo.services/files/" + identifier + "/" + itemIdentifier.substring(0, itemIdentifier.indexOf(".")) + "/" + tableName + ".fgb");
                    }
                    newAssetsObjList.add(newAssetObj);
                    //err.println(newAssetObj);
                }

                {
                    IomObject newAssetObj = new Iom_jObject(ASSET_STRUCTURE_TAG, null);
                    newAssetObj.setattrvalue("Identifier", tableName + ".parquet");
                    newAssetObj.setattrvalue("Title", tableName + " (GeoParquet)");
                    newAssetObj.setattrvalue("MediaType", "application/x-parquet");
                    if (identifier.equals(itemIdentifier)) {
                        newAssetObj.setattrvalue("Href", "http://stac.sogeo.services/files/" + identifier + "/" + tableName + ".parquet");
                    } else {
                        newAssetObj.setattrvalue("Href", "http://stac.sogeo.services/files" + identifier + "/" + itemIdentifier.substring(0, itemIdentifier.indexOf(".")) + "/" + tableName + ".parquet");
                    }
                    newAssetsObjList.add(newAssetObj);
                    //err.println(newAssetObj);
                }
            }

            Iom_jObject newItemObject = new Iom_jObject(ITEM_STRUCTURE_TAG, null);
            newItemObject.setattrvalue("Identifier", itemIdentifier);
            newItemObject.setattrvalue("Title", itemObj.getattrvalue("Title"));
            newItemObject.setattrvalue("Date", itemObj.getattrvalue("Date"));
            newItemObject.addattrobj("Boundary", itemObj.getattrobj("Boundary", 0));
            newItemObject.setattrvalue("Geometry", itemObj.getattrvalue("Geometry"));

            for (IomObject asset : newAssetsObjList) {
                newItemObject.addattrobj("Assets", asset);
            }
            //err.println(newItemObject);

            newItemsObjList.add(newItemObject);

            zipFile.delete();
            gpkgFile.delete();
        }

        Iom_jObject newCollectionObj = new Iom_jObject(iomObj.getobjecttag(), iomObj.getobjectoid());
        newCollectionObj.setattrvalue("Identifier", iomObj.getattrvalue("Identifier"));
        newCollectionObj.setattrvalue("Title", iomObj.getattrvalue("Title"));
        newCollectionObj.setattrvalue("ShortDescription", iomObj.getattrvalue("ShortDescription"));
        newCollectionObj.setattrvalue("Licence", iomObj.getattrvalue("Licence"));
        newCollectionObj.addattrobj("SpatialExtent", iomObj.getattrobj("SpatialExtent", 0));
        newCollectionObj.addattrobj("TemporalExtent", iomObj.getattrobj("TemporalExtent", 0));
        newCollectionObj.addattrobj("Owner", iomObj.getattrobj("Owner", 0));
        newCollectionObj.addattrobj("Servicer", iomObj.getattrobj("Servicer", 0));
        if (iomObj.getattrobj("Keywords", 0)!=null) {
            newCollectionObj.addattrobj("Keywords", iomObj.getattrobj("Keywords", 0));
        }
        for (IomObject item: newItemsObjList) {
            newCollectionObj.addattrobj("Items", item);
        }
        //err.println(newCollectionObj);

        return newCollectionObj;
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
            throw new IllegalArgumentException("INTERLIS compiler failed"); 
        }
        
        return iliTd;
    }
}
