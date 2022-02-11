package org.dde.zju.kczy;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.dde.zju.kczy.util.SecureShell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * The main class for parallel mineral prediction algorithm
 */
public class KCZYYC {

    private static final Logger logger = LoggerFactory.getLogger(KCZYYC.class);
    //    private static final String SSH_IP = "127.0.0.1";
    private static final String SSH_IP = "120.27.216.174";
    private static final int SSH_PORT = 22;
    private static final String SSH_USERNAME = "root";
    private static final String SSH_PASSWORD = "upxTP1oR5SIbeQKG";
    private static final String SSH_REMOTE_FILE_DIR = "/home/hulinshu/data/aster/"; //data in ssh computer(just for test)
    private static final String SSH_TEMP_VNIR_FILE_DIR = "/home/hulinshu/data/temp/vnir/";
    private static final String SSH_TEMP_SWIR_FILE_DIR = "/home/hulinshu/data/temp/swir/";
    private static final String SSH_TEMP_ARGILLIC_FILE_DIR = "/home/hulinshu/data/temp/argillic/";
    private static final String SSH_TEMP_SUB_FILE_DIR = "/home/hulinshu/data/temp/sub/";
    private static final String SSH_TEMP_RES_FILE_DIR = "/home/hulinshu/data/temp/res/";
    private static final String PROCESS_TOOLSET_DIR = "/home/hulinshu/tool/extractband_shibian/";
    private static final String FRAME_TOOLSET_DIR = "/home/hulinshu/tool/fengfu_tiantu/";
    private static final String PROJECT = "PROJCS[\\\"UTM_Zone_41N\\\",\n    GEOGCS[\\\"WGS 84\\\",\n        DATUM[\\\"World Geodetic System 1984\\\",\n            SPHEROID[\"WGS_84\",6378137,298.257223563]],\n        PRIMEM[\\\"Greenwich\\\",0],\n        UNIT[\\\"degree\\\",0.0174532925199433,\n            AUTHORITY[\\\"EPSG\\\",\\\"9122\\\"]]],\n    PROJECTION[\\\"Transverse_Mercator\\\"],\n    PARAMETER[\\\"latitude_of_origin\\\",0],\n    PARAMETER[\\\"central_meridian\\\",63],\n    PARAMETER[\\\"scale_factor\\\",0.9996],\n    PARAMETER[\\\"false_easting\\\",500000],\n    PARAMETER[\\\"false_northing\\\",0],\n    UNIT[\\\"metre\\\",1,\n        AUTHORITY[\\\"EPSG\\\",\\\"9001\\\"]],\n    AXIS[\\\"Easting\\\",EAST],\n    AXIS[\\\"Northing\\\",NORTH]]";

    private static final int RESOLUTION = 500; //分幅分辨率
    private static final int SUBWIDTH = 250; //分幅宽
    private static final int SUBHEIGHT = 200; //分幅高
    private static final float TWO_VALUE_THRESHOLD = 0.3f; //二值化分割阈值

    public static void main(String[] args) throws IOException {
        logger.info("======= Aster Mineral Prediction Start ======");

        // setup spark environment
        SparkSession ss = SparkSession
            .builder()
            .appName("mineral_prediction")
            .master("local[4]")
            .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(ss.sparkContext());

        String inputDir = args[0]; //local data dir
        File fileDir = new File(inputDir);
        if (fileDir.isFile()) {
            logger.error("Input should be a directory, exit");
            System.exit(1);
        }

        // Read data (*.hdf) => here is a hypothesis
        // prepare a filepath list
        List<String> filePaths = Arrays.asList(Objects.requireNonNull(fileDir.list()));
        filePaths = filePaths.stream().filter(x -> x.endsWith(".hdf")).collect(Collectors.toList());
        JavaRDD<String> filePathsRDD = jsc.parallelize(filePaths);
        filePathsRDD.cache();

        // Phase 1: Alteration
        // TODO data preparation (Extract band -> Crosstalk -> Radiometric calibration && Atmospheric correction)
        // step 1.1: export vnir
        JavaRDD<String> vnirFilePathsRDD = filePathsRDD.map((Function<String, String>) s -> {
            // TODO export vnir shell
            SecureShell secureShell = new SecureShell(SSH_IP, SSH_USERNAME, SSH_PASSWORD, SSH_PORT);
            String outputFileName = s.replace(".hdf", ".tif");
            String outputFilePath = SSH_TEMP_VNIR_FILE_DIR + outputFileName;
            String sshTempFile = SSH_TEMP_VNIR_FILE_DIR + s + ".sh";
            // prepare cmd
            StringBuilder sb = new StringBuilder("cd " + PROCESS_TOOLSET_DIR + "; wine ");
            sb.append("ExportVNIR.exe ");
            sb.append(SSH_REMOTE_FILE_DIR + s + " "); // 在manager为windows系统下使用
//                sb.append(inputDir + File.separator + s + " ");
            sb.append(outputFilePath);
            String result = secureShell.runWithOutput(sb.toString(), sshTempFile);
            logger.info(result);
            return outputFilePath;
        });

        // step 1.2: export swir
        JavaRDD<String> swirFilePathsRDD = filePathsRDD.map((Function<String, String>) s -> {
            // export swir shell
            SecureShell secureShell = new SecureShell(SSH_IP, SSH_USERNAME, SSH_PASSWORD, SSH_PORT);
            String outputFileName = s.replace(".hdf", ".tif");
            String outputFilePath = SSH_TEMP_SWIR_FILE_DIR + outputFileName;
            String sshTempFile = SSH_TEMP_SWIR_FILE_DIR + s + ".sh";
            // prepare cmd
            StringBuilder sb = new StringBuilder("cd " + PROCESS_TOOLSET_DIR + "; wine ");
            sb.append("ExportSWIR.exe ");
            sb.append(SSH_REMOTE_FILE_DIR + s + " ");
            sb.append(outputFilePath);
            String result = secureShell.runWithOutput(sb.toString(), sshTempFile);
            logger.info(result);
            return outputFilePath;
        });

        // step 1.3: Alteration
        JavaRDD<String> argillicFilePathsRDD = filePathsRDD.map((Function<String, String>) s -> {
            // export argillic shell
            SecureShell secureShell = new SecureShell(SSH_IP, SSH_USERNAME, SSH_PASSWORD, SSH_PORT);
            String outputFileName = s.replace(".hdf", ".tif");
            String outputFilePath = SSH_TEMP_ARGILLIC_FILE_DIR + outputFileName;
            String sshTempFile = SSH_TEMP_ARGILLIC_FILE_DIR + s + ".sh";
            // prepare cmd
            StringBuilder sb = new StringBuilder("cd " + PROCESS_TOOLSET_DIR + "; wine ");
            // function name
            sb.append("GetArgillic.exe ");
            // vnir filename
            sb.append(SSH_TEMP_VNIR_FILE_DIR + outputFileName + " ");
            // swir filename
            sb.append(SSH_TEMP_SWIR_FILE_DIR + outputFileName + " ");
            // output argillic filename
            sb.append(outputFilePath);

            String result = secureShell.runWithOutput(sb.toString(), sshTempFile);
            logger.info(result);
            return outputFilePath;
        }).persist(StorageLevel.MEMORY_AND_DISK_SER());

        // action
        vnirFilePathsRDD.collect().forEach(logger::info);
        swirFilePathsRDD.collect().forEach(logger::info);
        argillicFilePathsRDD.collect().forEach(logger::info);

        // Phase 2: framing and mapping
        // step 2.1: generate empty frames
        // calculate boundary
        JavaRDD<String> boundingBoxRDD = argillicFilePathsRDD.map((Function<String, String>) s -> {
            // get boundingBox
            SecureShell secureShell = new SecureShell(SSH_IP, SSH_USERNAME, SSH_PASSWORD, SSH_PORT);
            String sshTempFile = s + ".sh";
            StringBuilder sb = new StringBuilder("cd " + FRAME_TOOLSET_DIR + "; wine ");
            sb.append("GetSingleMapExtent.exe ");
            sb.append(s + " \"" + PROJECT + "\"");
            // return minx maxx maxy miny
            String boundingBox = secureShell.runWithOutput(sb.toString(), sshTempFile);
            logger.info(boundingBox);
            return boundingBox;
        });

        String maxBoundary = boundingBoxRDD.reduce((Function2<String, String, String>) (s, s2) -> {
            String[] split1 = s.split(" ");
            String[] split2 = s2.split(" ");

            double minX1 = Double.parseDouble(split1[0]);
            double minY1 = Double.parseDouble(split1[3]);
            double maxX1 = Double.parseDouble(split1[1]);
            double maxY1 = Double.parseDouble(split1[2]);

            double minX2 = Double.parseDouble(split2[0]);
            double minY2 = Double.parseDouble(split2[3]);
            double maxX2 = Double.parseDouble(split2[1]);
            double maxY2 = Double.parseDouble(split2[2]);

            double MIN_X = Double.min(minX1, minX2);
            double MIN_Y = Double.min(minY1, minY2);
            double MAX_X = Double.max(maxX1, maxX2);
            double MAX_Y = Double.max(maxY1, maxY2);

            return MIN_X + " " + MAX_X + " " + MAX_Y + " " + MIN_Y;
        });

        String[] boundary = maxBoundary.split(" ");
        double minX = Double.parseDouble(boundary[0]);
        double minY = Double.parseDouble(boundary[3]);
        double maxX = Double.parseDouble(boundary[1]);
        double maxY = Double.parseDouble(boundary[2]);

        // give boundary to generate empty frames
        SecureShell shell = new SecureShell(SSH_IP, SSH_USERNAME, SSH_PASSWORD, SSH_PORT);
        String sshTempFile = SSH_TEMP_ARGILLIC_FILE_DIR + "generate.sh";
        StringBuilder sb = new StringBuilder("cd " + FRAME_TOOLSET_DIR + "; wine ");
        sb.append("GetMapSubFileList.exe ");
        sb.append(minX + " " + maxX + " " + maxY + " " + minY + " " + RESOLUTION + " " + SUBWIDTH + " " + SUBHEIGHT);
        String res = shell.runWithOutput(sb.toString(), sshTempFile); // get emptyframes filenames;
        List<String> subFileList = Arrays.asList(res.split(" "))
            .stream().filter(f -> f.endsWith(".TIF")).collect(Collectors.toList());
        JavaRDD<String> subFilesRDD = jsc.parallelize(subFileList).persist(StorageLevel.MEMORY_AND_DISK_SER());
        subFilesRDD.map((Function<String, String>) s -> {
            // export shell
            SecureShell secureShell = new SecureShell(SSH_IP, SSH_USERNAME, SSH_PASSWORD, SSH_PORT);
            String sshTempFile1 = SSH_TEMP_SUB_FILE_DIR + s + ".sh";
            // prepare cmd
            StringBuilder stringBuilder = new StringBuilder("cd " + FRAME_TOOLSET_DIR + "; wine ");
            // function name
            stringBuilder.append("CreateSubImg ");
            stringBuilder.append(minX + " " + maxY + " " + RESOLUTION + " " + SUBWIDTH + " " + SUBHEIGHT + " "
                + "\"" + PROJECT + "\"" + " " + SSH_TEMP_SUB_FILE_DIR + " " + s);
            secureShell.runWithOutput(stringBuilder.toString(), sshTempFile1);
            logger.info("CreateSubImg: " + s);
            return s;
        }).collect().forEach(logger::info);

        // step 2.2: fill-in the frames
        // step 2.2.1: generate fill-in index
        String sshTempFile2 = SSH_TEMP_SUB_FILE_DIR + "index.sh";
        String indexPath = SSH_TEMP_SUB_FILE_DIR + "index.txt";

        sb = new StringBuilder("cd " + FRAME_TOOLSET_DIR + "; wine ");
        sb.append("CreateIndexFile.exe ");
        sb.append(SSH_TEMP_SUB_FILE_DIR + " \"" + PROJECT + "\" " + indexPath);
        res = shell.runWithOutput(sb.toString(), sshTempFile2);

        // step 2.2.2: start to fill map
        argillicFilePathsRDD.map((Function<String, String>) s -> {
            // export shell
            SecureShell secureShell = new SecureShell(SSH_IP, SSH_USERNAME, SSH_PASSWORD, SSH_PORT);
            String sshTempFile1 = SSH_TEMP_SUB_FILE_DIR + s + ".sh";
            // prepare cmd
            StringBuilder stringBuilder = new StringBuilder("cd " + FRAME_TOOLSET_DIR + "; wine ");
            // function name
            stringBuilder.append("FillMapbySingleFile ");
            stringBuilder.append(s + " " + indexPath);
            String result = secureShell.runWithOutput(stringBuilder.toString(), sshTempFile1);
            logger.info("FillMapbySingleFile: " + s);
            return s;
        }).collect().forEach(logger::info);

        // step 2.2.3: start to generate binary image
        subFilesRDD.map((Function<String, String>) s -> {
            // export shell
            SecureShell secureShell = new SecureShell(SSH_IP, SSH_USERNAME, SSH_PASSWORD, SSH_PORT);
            String sshTempFile1 = SSH_TEMP_RES_FILE_DIR + s + ".sh";
            String inputFilePath = SSH_TEMP_SUB_FILE_DIR + s;
            String outputFilePath = SSH_TEMP_RES_FILE_DIR + s;
            // prepare cmd
            StringBuilder stringBuilder = new StringBuilder("cd " + FRAME_TOOLSET_DIR + "; wine ");
            // function name
            stringBuilder.append("GetResbySingleFile ");
            stringBuilder.append(TWO_VALUE_THRESHOLD + " " + inputFilePath + " " + outputFilePath);
            secureShell.runWithOutput(stringBuilder.toString(), sshTempFile1);
            logger.info("GetResbySingleFile: " + s);
            return s;
        }).collect().forEach(logger::info);


        ss.stop();
        ss.close();
        logger.info("======= Aster Mineral Prediction End ======");
    }
}
