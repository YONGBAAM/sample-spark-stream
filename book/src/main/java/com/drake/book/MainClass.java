package com.drake.book;

import com.beust.jcommander.Parameter;
import com.google.gson.Gson;
import jdk.jfr.internal.tool.Main;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Int;
import scala.Tuple2;

import java.io.FileOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

public class MainClass implements Serializable {

    @Parameter(names = {"--app-name", "-n", "--name"})
    private String appName = "sample app";


    FlatMapFunction<String, String> sampleFlatMapFunction = new FlatMapFunction<String, String>() {
        @Override
        public Iterator<String> call(String x) {
            return Arrays.stream(x.split(" ")).iterator();
        }
    };

//        Function2<Integer, Integer, Integer> sampleAggFunction = (integer, integer2) -> integer + integer2;

    Function2<Integer, Integer, Integer> sampleAggFunction = new Function2<Integer, Integer, Integer>() {
        @Override
        public Integer call(Integer integer, Integer integer2) {
            return integer + integer2;
        }
    };

//    Function<String, Tuple2<String, Integer>> sampleExpandFunction = st -> new Tuple2<>(st, 1);
    Function<String, Tuple2<String, Integer>> sampleExpandFunction
            = new Function<String, Tuple2<String, Integer>>() {
        @Override
        public Tuple2<String, Integer> call(String st) {
            return new Tuple2<>(st, 1);
        }
    };


    PairFunction<String, String, Integer> samplePairFunction =
            new PairFunction<String, String, Integer>() {
        public Tuple2<String, Integer> call(String st){
            return new Tuple2<>(st, 1);

        };

            };

    String root = "folder";

    public void exampleOutputJson(JavaRDD<WordJoin> persistedRdd, String outName) {
        persistedRdd.foreach(x -> {
            final Gson gson =new Gson();
            String root = this.root.equals("") ? "" : this.root + "/";
            String binaryOutputPath = root + outName + "_" + x.getWord();
            String jsonOutputPath = binaryOutputPath + ".txt";

            try (FileOutputStream binaryOutputStream = new FileOutputStream(binaryOutputPath)) {
                binaryOutputStream.write(x.toByteArray());
            } catch (Exception e) {
                // TODO: use slf4j
                System.out.println(e.toString());
            }

            try (FileOutputStream jsonWriteStream = new FileOutputStream(jsonOutputPath)) {
                jsonWriteStream.write(gson.toJson(x).getBytes());
            } catch (Exception e) {
                // TODO: use slf4j
                System.out.println(e.toString());
            }

        });
    }

    private void exampleCombineByKey(JavaPairRDD<String, Integer> wordCountRdd) {

        // word count
        JavaPairRDD<String, Integer> firstCharacterCountRdd =
                wordCountRdd.filter(tu -> tu._1.length()>0)
                .mapToPair(s
                -> new Tuple2<String, Integer>(s._1.substring(0, 1), s._2));

        // count, sum
        JavaPairRDD<String, Tuple2<Integer, Double>> firstCharacterCountSumRdd = firstCharacterCountRdd.combineByKey(
                // init value
                val -> new Tuple2<Integer, Double>(1, (double) val),

                //accumulator
                (tu, val) -> new Tuple2<Integer, Double>(tu._1 + 1, tu._2 + val),

                // merge of partition
                (x, y) -> new Tuple2<Integer, Double>(x._1 + y._1, x._2 + y._2)
        );

        firstCharacterCountSumRdd.foreach(
                x -> System.out.println(x.toString())
        );
    }



    public static void main(String[] args) {
//        JavaSparkContext sc = new JavaSparkContext(
//                new SparkConf().setMaster("local[2]").setAppName("appName")
//        );
//
//        JavaRDD<String> input = sc.textFile("README.md");
//
//
//        JavaPairRDD<String, Integer> join = input.flatMap(x -> Arrays.stream(x.split(" ")).iterator())
//                .mapToPair(x -> new Tuple2<>(x, 1)).reduceByKey(Integer::sum);
//
//
//        join.foreach(x -> System.out.println(x.toString()));

        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setMaster("local").setAppName("README.md")
        );
        JavaRDD<String> lines = sc.textFile("README.md");
        MainClass in = new MainClass();
        JavaRDD<WordJoin> rrr = Processors.makeWordCountRdd(lines);
        rrr.persist(StorageLevel.MEMORY_AND_DISK());
        in.exampleOutputJson(rrr, "readme");

    }

    private JavaPairRDD<String, Integer> makeJoinWithCall(JavaRDD<String> lines) {

//        FlatMapFunction<String, String> sampleFlatMapFunction = x -> Arrays.stream(x.split(" ")).iterator();


        return lines.map(x -> x.replaceAll("[^a-zA-Z0-9]"," ")).flatMap(sampleFlatMapFunction)
                .mapToPair(samplePairFunction).reduceByKey(sampleAggFunction);

    }

    private JavaPairRDD<String, Integer> makeJoinWithSpecificFunction(String fileName) {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setMaster("local").setAppName(fileName)
        );
        JavaRDD<String> lines = sc.textFile(fileName);

        Map<String, Long> ret = lines.countByValue();
//        FlatMapFunction<String, String> sampleFlatMapFunction = x -> Arrays.stream(x.split(" ")).iterator();


        return lines.flatMap(sampleFlatMapFunction).mapToPair(samplePairFunction).reduceByKey(sampleAggFunction);

    }


    private static class Contains implements Function<String, Boolean> {

        public Contains(String keyword) {
            this.keyword = keyword;
        }
        String keyword;

        public Boolean call(String st) {
            return st.toLowerCase().contains("error");
        }
    }
}



