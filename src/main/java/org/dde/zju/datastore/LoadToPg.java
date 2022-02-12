package org.dde.zju.datastore;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;
import org.dde.zju.datastore.proto.DataItem;
import org.dde.zju.kczy.KCZYYC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class LoadToPg {

    private static final Logger logger = LoggerFactory.getLogger(KCZYYC.class);
    private static final String PG_URL = "";
    private static final String USERNAME = "";
    private static final String PASSWORD = "";
    private static final String TABLE_NAME = "";

    public static void main(String[] args) {

        String infile = args[0];
        // setup spark environment
        SparkSession ss = SparkSession
                .builder()
                .appName("load data to pg in parallel")
                .master("local[4]")
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(ss.sparkContext());

        File fileDir = new File(infile); // directory or file
        if (!fileDir.exists()) {
            logger.error("File or Directory Not Exist");
            System.exit(1);
        }

        JavaRDD<DataItem> inDataRDD = jsc.textFile(infile).map(x -> new DataItem(x));
        inDataRDD.cache();
        long totalCount = inDataRDD.count();
        JavaRDD<Long> outDataRDD = inDataRDD.mapPartitions(new FlatMapFunction<Iterator<DataItem>, Long>() {
            @Override
            public Iterator<Long> call(Iterator<DataItem> indataItems) throws Exception {
                    Class<?> driver = Class.forName("org.postgresql.Driver");
                    Connection connection= DriverManager.getConnection(PG_URL, USERNAME, PASSWORD);
                    PreparedStatement pstmt = connection.prepareStatement(
                            "insert into " + TABLE_NAME + " values(unnest(?))"
                    );
                    long itemNum = 0;
                    while(indataItems.hasNext()) {
                        DataItem di = indataItems.next();
                        pstmt.setString(1, di.toString());
                        itemNum ++;
                    }

                int update = pstmt.executeUpdate();
                pstmt.close();
                connection.close();

                String str = "============== \n Partition Total: " + itemNum + "; \n Success Total: " + update + "; \n";
                logger.info(str);
                List<Long> resultArr = new ArrayList<>();
                resultArr.add(itemNum);
                return resultArr.iterator();
            };
        });

       Long resultAll = outDataRDD.reduce(Long::sum);
       logger.info("============== \n Total: " + totalCount + "; \n Success Total: " + resultAll + "; \n");
    }

}
