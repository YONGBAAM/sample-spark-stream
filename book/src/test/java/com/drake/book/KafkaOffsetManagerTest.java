package com.drake.book;

import org.apache.spark.streaming.kafka010.OffsetRange;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class KafkaOffsetManagerTest {

    KafkaOffsetManager offsetManager;

    @BeforeEach
    public void init() {
        offsetManager = KafkaOffsetManager.builder()
                .root(".").checkpointDirectory("testCheckpoints").markerDirectory("testMarkers")
                .build();

        // TODO: makedir in offset manager?
        new File("./testCheckpoints").mkdirs();
        new File("./testMarkers").mkdirs();
    }

    @Test
    public void test() throws IOException {

        final int time = 14141414;
        final String topic = "sample.topic.1";
        OffsetRange[] offsetRanges ={
                OffsetRange.create(topic,0, 11, 22),
                OffsetRange.create(topic,1, 12, 23),
                OffsetRange.create(topic,2, 14, 24),


        };
        offsetManager.makeCheckpoints(time, topic, offsetRanges);
        offsetManager.commitOffsets(time, topic);
        Set<OffsetRange> readOffsets = Arrays.stream(offsetManager.readOffsets(topic)).collect(Collectors.toSet());
        Set<OffsetRange> expectedOffsets = Arrays.stream(offsetRanges).collect(Collectors.toSet());
        assertEquals(readOffsets, expectedOffsets);
    }

}