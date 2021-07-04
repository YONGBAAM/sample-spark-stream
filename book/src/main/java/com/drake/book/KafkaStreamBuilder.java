package com.drake.book;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

public class KafkaStreamBuilder {

    private final Logger LOG = LoggerFactory.getLogger(KafkaStreamBuilder.class);

    private KafkaOffsetManager offsetManager;
    private JavaStreamingContext streamingContext;
    private Map<String, Object> kafkaParamMap;
    private String topicId;

    public KafkaStreamBuilder setOffsetManager(KafkaOffsetManager offsetManager) {
        this.offsetManager = offsetManager;
        return this;
    }

    public KafkaStreamBuilder setStreamingContext(JavaStreamingContext streamingContext) {
        this.streamingContext = streamingContext;
        return this;
    }

    public KafkaStreamBuilder setKafkaParamMap(Map<String, Object> kafkaParamMap) {
        this.kafkaParamMap = kafkaParamMap;
        return this;
    }

    public KafkaStreamBuilder setTopicId(String topicId) {
        this.topicId = topicId;
        return this;
    }

    // TODO: stream all in once?
    public JavaDStream<byte[]> build() {
        Map<TopicPartition, Long> offsets = offsetManager.readOffsetsTopicPartition(topicId);

        JavaInputDStream<ConsumerRecord<String, byte[]>> stream;
        if ( offsets == null) {
            // directstream(context, locationst, cunsumerst(topics, kafkaparams, (offset))
            stream =
                    KafkaUtils.createDirectStream(streamingContext,
                            LocationStrategies.PreferConsistent(),
                            ConsumerStrategies.Subscribe(
                                    Collections.singleton(topicId), kafkaParamMap)
                    );
        }
        else{
            stream =
                    KafkaUtils.createDirectStream(streamingContext,
                            LocationStrategies.PreferConsistent(),
                            ConsumerStrategies.Subscribe(
                                    Collections.singleton(topicId), kafkaParamMap, offsets)
                    );

        }


        // https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html
        stream.foreachRDD((rdd, time) -> {

            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            offsetManager.makeCheckpoints(time.milliseconds(), topicId, offsetRanges);

        });

        return stream.map(ConsumerRecord::value);

    }

}
