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
//    private static final String PG_URL = "jdbc:postgresql://pgm-bp18r1dq5gq2b8w7po.pg.rds.aliyuncs.com:1921/fuxi?TimeZone=Asia/Shanghai";
//    private static final String USERNAME = "deepengine_1";
//    private static final String PASSWORD = "upxTP1oR5SIbeQKG";
    private static final String PG_URL = "jdbc:postgresql://gp-bp194h35k6cyg89e5o-master.gpdb.rds.aliyuncs.com:5432/fuxi?TimeZone=Asia/Shanghai";
    private static final String USERNAME = "zjugis";
    private static final String PASSWORD = "1qaz@WSX";
    private static final String TABLE_NAME = "fx_raster";

    public static void main(String[] args) {

        String infile = args[0];
        // setup spark environment
        SparkSession ss = SparkSession
                .builder()
                .appName("load data to pg in parallel")
                .master("local[16]")
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(ss.sparkContext());

        File fileDir = new File(infile); // directory or file
        if (!fileDir.exists()) {
            logger.error("File or Directory Not Exist");
            System.exit(1);
        }

        JavaRDD<DataItem> inDataRDD = jsc.textFile(infile).map(x -> new DataItem(x));
        inDataRDD.cache();
//        inDataRDD.collect().forEach(System.out::println);
        long totalCount = inDataRDD.count();
        JavaRDD<Long> outDataRDD = inDataRDD.mapPartitions(new FlatMapFunction<Iterator<DataItem>, Long>() {
            @Override
            public Iterator<Long> call(Iterator<DataItem> indataItems) throws Exception {
                Class<?> driver = Class.forName("org.postgresql.Driver");
                Connection connection = DriverManager.getConnection(PG_URL, USERNAME, PASSWORD);
                PreparedStatement pstmt = connection.prepareStatement(
                        "insert into " + TABLE_NAME + " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                );
                long itemNum = 0;
                while (indataItems.hasNext()) {
                    DataItem di = indataItems.next();
                    pstmt.setString(1, di.getSatellite());
                    pstmt.setString(2, di.getDataUrl());
                    pstmt.setString(3, di.getLevel());
                    pstmt.setString(4, di.getLoadInfo());
                    pstmt.setString(5, di.getResolution());
                    pstmt.setString(6, di.getImageUrl());
                    pstmt.setLong(7, di.getTimestamp());
                    pstmt.setString(8, di.getImageId());
                    pstmt.setString(9, di.getProvider());
                    pstmt.setString(10, di.getNameEn());
                    pstmt.setInt(11, di.getBandCount());
                    pstmt.setString(12, di.getSensor());
                    pstmt.setString(13, di.getBoundary());
                    pstmt.setDouble(14, di.getCloud());
                    pstmt.setString(15, "ADMIN");
                    pstmt.setString(16, "PUBLIC");
                    pstmt.setBoolean(17, false);
                    itemNum++;
                    try{
                        int update = pstmt.executeUpdate();
                    } catch (Exception e){

                    }
                }

                pstmt.close();
                connection.close();

                String str = "============== \n Partition Total: " + itemNum + "; \n Success Total: " + "update" + "; \n";
                logger.info(str);
                List<Long> resultArr = new ArrayList<>();
                resultArr.add(itemNum);
                return resultArr.iterator();
            }

            ;
        });

        Long resultAll = outDataRDD.reduce(Long::sum);
        logger.info("============== \n Total: " + totalCount + "; \n Success Total: " + resultAll + "; \n");
    }

}
