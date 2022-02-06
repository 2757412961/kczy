package org.dde.zju.kczy;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.dde.zju.kczy.util.SecureShell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple4;

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
    private static final String SSH_TEMP_FRAME_FILE_DIR = "/home/hulinshu/data/temp/frames/";
    private static final String SSH_TEMP_FILL_FILE_DIR = "/home/hulinshu/data/temp/fill/";
    private static final String PROCESS_TOOLSET_DIR = "/home/hulinshu/tool/extractband_shibian/";
    private static final String FRAME_TOOLSET_DIR = "/home/hulinshu/tool/fengfu_tiantu/";

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

        // String outDir = args[1];

        // Read data (*.hdf) => here is a hypothesis
        // prepare a filepath list
        List<String> filePaths = Arrays.asList(Objects.requireNonNull(fileDir.list()));
        filePaths = filePaths.stream().filter(x -> x.endsWith(".hdf")).collect(Collectors.toList());
        JavaRDD<String> filePathsRDD = jsc.parallelize(filePaths);
        filePathsRDD.cache();

        // Phase 1: Alteration
        // TODO data preparation (Extract band -> Crosstalk -> Radiometric calibration && Atmospheric correction)
        // step 1.1: export vnir
        JavaRDD<String> vnirFilePathsRDD = filePathsRDD.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                // TODO export vnir shell
                SecureShell ss = new SecureShell(SSH_IP, SSH_USERNAME, SSH_PASSWORD, SSH_PORT);
                String outputFileName = s.replace(".hdf", ".tif");
                String outputFilePath = SSH_TEMP_VNIR_FILE_DIR + outputFileName;
                String sshTempFile = SSH_TEMP_VNIR_FILE_DIR + s + ".sh";
                // prepare cmd
                StringBuilder sb = new StringBuilder("cd " + PROCESS_TOOLSET_DIR + "; wine ");
                sb.append("ExportVNIR.exe ");
                sb.append(SSH_REMOTE_FILE_DIR + s + " "); // 在manager为windows系统下使用
//                sb.append(inputDir + File.separator + s + " ");
                sb.append(outputFilePath);
                String result = ss.runWithOutput(sb.toString(), sshTempFile);
                logger.info(result);
                return outputFilePath;
            }
        });

        // step 1.2: export swir
        JavaRDD<String> swirFilePathsRDD = filePathsRDD.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                // export swir shell
                SecureShell ss = new SecureShell(SSH_IP, SSH_USERNAME, SSH_PASSWORD, SSH_PORT);
                String outputFileName = s.replace(".hdf", ".tif");
                String outputFilePath = SSH_TEMP_SWIR_FILE_DIR + outputFileName;
                String sshTempFile = SSH_TEMP_SWIR_FILE_DIR + s + ".sh";
                // prepare cmd
                StringBuilder sb = new StringBuilder("cd " + PROCESS_TOOLSET_DIR + "; wine ");
                sb.append("ExportSWIR.exe ");
                sb.append(SSH_REMOTE_FILE_DIR + s + " ");
//                sb.append(inputDir + File.separator + s + " ");
                sb.append(outputFilePath);
                String result = ss.runWithOutput(sb.toString(), sshTempFile);
                logger.info(result);
                return outputFilePath;
            }
        });

        // step 1.3: Alteration
        JavaRDD<String> argillicFilePathsRDD = filePathsRDD.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                // export argillic shell
                SecureShell ss = new SecureShell(SSH_IP, SSH_USERNAME, SSH_PASSWORD, SSH_PORT);
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

                String result = ss.runWithOutput(sb.toString(), sshTempFile);
                logger.info(result);
                return outputFilePath;
            }
        });


        // Phase 2: raming and mapping
        // TODO generate empty frames
        // calculate boundary
        JavaRDD<Object> boundingBoxRDD = argillicFilePathsRDD.map(new Function<String, Object>() {
            @Override
            public Object call(String s) throws Exception {
                // TODO get boundingBox
                SecureShell ss = new SecureShell(SSH_IP, SSH_USERNAME, SSH_PASSWORD, SSH_PORT);
                String sshTempFile = s + ".sh";
                StringBuilder sb = new StringBuilder("cd " + FRAME_TOOLSET_DIR + "; wine ");
                sb.append("GetBoudary.exe ");
                sb.append(s + " ");
                // assume minx, miny, maxx, maxy
                String boundingBox = ss.runWithOutput(sb.toString(), sshTempFile);
                return boundingBox;
            }
        }).mapToPair((PairFunction<Object, Object, Object>) o -> new Tuple2<>("key", o)).groupByKey()
            .map((Function<Tuple2<Object, Iterable<Object>>, Object>) objectIterableTuple2 -> {
                double MIN_X = Double.MAX_VALUE;
                double MIN_Y = Double.MAX_VALUE;
                double MAX_X = Double.NEGATIVE_INFINITY;
                double MAX_Y = Double.NEGATIVE_INFINITY;

                for (Object o : objectIterableTuple2._2) {
                    String[] s1 = ((String) o).split(" ");
                    double minX = Double.parseDouble(s1[0]);
                    double minY = Double.parseDouble(s1[1]);
                    double maxX = Double.parseDouble(s1[2]);
                    double maxY = Double.parseDouble(s1[3]);

                    MIN_X = Double.min(minX, MIN_X);
                    MIN_Y = Double.min(minY, MIN_Y);
                    MAX_X = Double.max(maxX, MAX_X);
                    MAX_Y = Double.max(maxY, MAX_Y);
                }

                return new Tuple4<>(MIN_X, MIN_Y, MAX_X, MAX_Y);
            });

        List<Object> boundingList = boundingBoxRDD.collect();
        Double minX = (Double) ((Tuple4) boundingList.get(0))._1();
        Double minY = (Double) ((Tuple4) boundingList.get(0))._2();
        Double maxX = (Double) ((Tuple4) boundingList.get(0))._3();
        Double maxY = (Double) ((Tuple4) boundingList.get(0))._4();

        // TODO give boundary to generate empty frames
        SecureShell shell = new SecureShell(SSH_IP, SSH_USERNAME, SSH_PASSWORD, SSH_PORT);
        String sshTempFile = SSH_TEMP_ARGILLIC_FILE_DIR + "generate.sh";
        StringBuilder sb = new StringBuilder("cd " + FRAME_TOOLSET_DIR + "; wine ");
        sb.append("GetMapSubdivision.exe ");
        sb.append("'" + minX + " " + minY + " " + maxX + " " + maxY + "' "); // boundingbox
        sb.append("$dResolution $uiSubWid $uiSubHeight");
        String res = shell.runWithOutput(sb.toString(), sshTempFile); // get emptyframes filenames;

        List<String> frames = Arrays.asList(res.split(" "));
        JavaRDD<String> framesFilePathsRDD = jsc.parallelize(frames);

        // TODO fill-in the frames
        JavaRDD<String> fillFilePathsRDD = framesFilePathsRDD.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                // TODO fill
                // export shell
                SecureShell ss = new SecureShell(SSH_IP, SSH_USERNAME, SSH_PASSWORD, SSH_PORT);
                String outputFileName = s.substring(s.lastIndexOf("/") + 1);
                String outputFilePath = SSH_TEMP_FILL_FILE_DIR + outputFileName;
                String sshTempFile = SSH_TEMP_FILL_FILE_DIR + s + ".sh";
                // prepare cmd
                StringBuilder sb = new StringBuilder("cd " + FRAME_TOOLSET_DIR + "; wine ");
                // function name
                sb.append("GetBinaryFilledMap ");

                return outputFileName;
            }
        });

        // action
        vnirFilePathsRDD.collect().forEach(logger::info);
        swirFilePathsRDD.collect().forEach(logger::info);
        argillicFilePathsRDD.collect().forEach(logger::info);
        fillFilePathsRDD.collect().forEach(logger::info);

        ss.stop();
        ss.close();
        logger.info("======= Aster Mineral Prediction End ======");
    }
}
