package com.drake.book;

import com.google.gson.Gson;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.FileOutputStream;
import java.io.Serializable;
import java.util.Arrays;

public class Processors implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(Processors.class);

    // Write to local instead of Hbase
    public static void writeToLocal(JavaRDD<WordJoin> persistedRdd, String root, String outName) {
        persistedRdd.foreach(x -> {
            final Gson gson = new Gson();
            String binaryOutputPath = (root.equals("") ? "" : root + "/") + outName + "_" + x.getWord();
            String jsonOutputPath = binaryOutputPath + ".txt";

            try (FileOutputStream binaryOutputStream = new FileOutputStream(binaryOutputPath)) {
                binaryOutputStream.write(x.toByteArray());
            } catch (Exception e) {
                LOG.warn("WordJoin write failed : " + e.toString());
                System.out.println(e.toString());
            }

            try (FileOutputStream jsonWriteStream = new FileOutputStream(jsonOutputPath)) {
                jsonWriteStream.write(gson.toJson(x).getBytes());
            } catch (Exception e) {
                LOG.warn("Json write failed : " + e.toString());
            }
        });
    }

    public static JavaRDD<WordJoin> makeWordCountRdd(JavaRDD<String> lines) {
        return lines.map(x -> x.replaceAll("[^a-zA-Z0-9]", " "))
                .flatMap(x -> Arrays.stream(x.split(" ")).iterator())
                .mapToPair(x -> new Tuple2<String, Integer>(x, 1))
                .reduceByKey(Integer::sum)
                .filter(tu -> !tu._1.equals(""))
                .map(tu -> WordJoin.newBuilder().setWord(tu._1).setCount(tu._2).build());
    }
}



