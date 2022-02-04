package org.dde.zju.kczy;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.dde.zju.kczy.util.SecureShell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * The main class for parallel mineral prediction algorithm
 */
public class KCZYYC {

    private static final Logger logger = LoggerFactory.getLogger(KCZYYC.class);
    private static final String SSH_IP = "127.0.0.1";
    private static final int SSH_PORT = 22;
    private static final String SSH_USERNAME = "hulinshu";
    private static final String SSH_PASSWORD = "hu";
    private static final String SSH_TEMP_FILE_DIR = "/home/hulinshu/data/temp/";
    private static final String PROCESS_TOOLSET_DIR = "/home/hulinshu/tool/extractband_shibian/";
    private static final String FRAME_TOOLSET_DIR = "/home/hulinshu/tool/fengfu_tiantu/";

    public static void main(String[] args) {
       logger.info("======= Aster Mineral Prediction Start ======");

       // setup spark environment
        SparkSession ss = SparkSession
                .builder()
                .appName("mineral_prediction")
                .master("local[4]")
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(ss.sparkContext());

        String inputDir = args[0];
        File fileDir = new File(inputDir);
        if (fileDir.isFile()) {
            logger.error("Input should be a directory, exit");
            System.exit(1);
        }

        // String outDir = args[1];

        // Read data (*.hdf) => here is a hypothesis
        // prepare a filepath list
        List<String> filePaths = Arrays.asList(Objects.requireNonNull(fileDir.list()));
        filePaths = filePaths.stream().filter(x->x.endsWith(".hdf")).collect(Collectors.toList());
        JavaRDD<String> filePathsRDD = jsc.parallelize(filePaths);
        filePathsRDD.cache();

        // Phrase 1: Alteration
        // TODO data preparation (Extract band -> Crosstalk -> Radiometric calibration && Atmospheric correction)
        // step 1.1: export vnir
        JavaRDD<String> vnirFilePathsRDD = filePathsRDD.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                // TODO export vnir shell
                SecureShell ss = new SecureShell(SSH_IP, SSH_USERNAME, SSH_PASSWORD, SSH_PORT);
                String outputFileName = s.replace(".hdf", ".tif");
                String outputFilePath = SSH_TEMP_FILE_DIR + "vnir/" + outputFileName;
                String sshTempFile = SSH_TEMP_FILE_DIR + "vnir/" + s + ".sh";
                // prepare cmd
                StringBuilder sb = new StringBuilder("cd " + PROCESS_TOOLSET_DIR + "; wine ");
                sb.append("ExportVNIR.exe ");
                sb.append(inputDir + File.separator + s + " ");
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
                // TODO export swir shell
                SecureShell ss = new SecureShell(SSH_IP, SSH_USERNAME, SSH_PASSWORD, SSH_PORT);
                String outputFileName = s.replace(".hdf", ".tif");
                String outputFilePath = SSH_TEMP_FILE_DIR + "swir/" + outputFileName;
                String sshTempFile = SSH_TEMP_FILE_DIR + "swir/" + s + ".sh";
                // prepare cmd
                StringBuilder sb = new StringBuilder("cd " + PROCESS_TOOLSET_DIR + "; wine ");
                sb.append("ExportSWIR.exe ");
                sb.append(inputDir + File.separator + s + " ");
                sb.append(outputFilePath);
                String result = ss.runWithOutput(sb.toString(), sshTempFile);
                logger.info(result);
                return outputFilePath;
            }
        });

        // TODO band data pre-processing

        // TODO Alteration

        // Phrase 2: raming and mapping
        // TODO generate empty frames

        // TODO fill-in the frames
        vnirFilePathsRDD.collect().forEach(logger::info);
        swirFilePathsRDD.collect().forEach(logger::info);

        ss.stop();
        ss.close();
        logger.info("======= Aster Mineral Prediction End ======");
    }
}
