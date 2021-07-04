package com.drake.book;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.Maps;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

public class SparkStreamer {

    @Parameter(names = "--topic")
    String topic = "test-wordcount";

    @Parameter(names = "--endpoint", required = false)
    String endpoint = "";

    private static final int ONE_HOUR_MS = 60*60*1000;

    private static final Logger LOG = LoggerFactory.getLogger(SparkStreamer.class);

    public static void main(String[] args) {

        SparkStreamer streamer = new SparkStreamer();
        new JCommander(streamer, args);
        streamer.run();
    }

    public void run() {


        KafkaOffsetManager offsetManager = KafkaOffsetManager.builder()
                .root(".").checkpointDirectory("checkpoints").markerDirectory("markers")
                .build();
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setMaster("local").setAppName("word-count")
        );

        JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(60));
        // JavaStreamingcontext
        jsc.addStreamingListener(StreamingBatchListener.builder()
                .dataSourceIds(Collections.singletonList(topic))
                .kafkaOffsetManager(offsetManager)
                .build()
        );
        Map<String, Object> kafkaParams = Maps.newHashMap();
        kafkaParams.put("group.id", "test");
        kafkaParams.put("bootstrap.servers", endpoint);
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", "false");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", ByteArrayDeserializer.class);

        LOG.info("kafka params: " + kafkaParams.toString());

        JavaDStream<byte[]> stream = new KafkaStreamBuilder().setKafkaParamMap(kafkaParams)
                .setStreamingContext(jsc).setTopicId(topic).setOffsetManager(offsetManager).build();

        genUpdate(stream);

        jsc.start();
        try{
            boolean result = jsc.awaitTerminationOrTimeout(ONE_HOUR_MS);

        } catch (InterruptedException e) {
            LOG.error(e.toString());
        }

    }

    private void genUpdate(JavaDStream<byte[]> stream) {
        stream.foreachRDD(rdd -> {

            JavaRDD<WordJoin> wordJoinRdd = Processors.makeWordCountRdd(rdd.map(String::new));
            // TODO: set title of file as the outName

            Processors.writeToLocal(wordJoinRdd, "./output", "word");

        });
    }
}
