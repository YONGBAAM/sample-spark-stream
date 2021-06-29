package com.drake.book;

import lombok.Builder;
import lombok.NonNull;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.streaming.kafka010.OffsetRange;

import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KafkaOffsetManager {
    private final String root;
    private final String checkpointDirectory;
    private final String markerDirectory;

    @Builder
    private KafkaOffsetManager(@NonNull String root, String checkpointDirectory, String markerDirectory) {

        this.root = root;
        this.checkpointDirectory = checkpointDirectory;
        this.markerDirectory = markerDirectory;
    }


    // Json으로 받기


    private String serializeOffsetRanges(OffsetRange[] ranges) {
        Stream<OffsetRange> stream = Stream.of(ranges);

        return stream.sorted(
                Comparator.comparingInt(OffsetRange::partition)
        ).map(
                r -> "" + r.topic() + ":" + r.partition() + ":" + r.fromOffset() + ":" + r.untilOffset()
        ).collect(
                Collectors.joining("\n")
        );

    }

    private Map<TopicPartition, Integer> deSerializeOffsetRanges(String offsetLines) {
        Stream<String> stream = Stream.of(offsetLines.split("\n"));

        return stream
                .map(line -> line.split(":"))
                .collect(
                        Collectors.toMap(
                                array -> new TopicPartition(array[0], Integer.parseInt(array[1])),
                                array -> Integer.parseInt(array[3])
                        )
                ); // collect end
    }
}
