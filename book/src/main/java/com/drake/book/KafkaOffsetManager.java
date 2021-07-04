package com.drake.book;

import com.google.gson.Gson;
import lombok.Builder;
import lombok.NonNull;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.Thread.sleep;
/*
*   Mock S3 writer in local
* */
public class KafkaOffsetManager {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaOffsetManager.class);

    private final String root;
    private final String checkpointPrefix;
    private final String markerPrefix;

    @Builder
    public KafkaOffsetManager(@NonNull String root, String checkpointDirectory, String markerDirectory) {

        this.root = root;
        this.checkpointPrefix = checkpointDirectory;
        this.markerPrefix = markerDirectory;
    }


    // TODO: time to long?
    public void makeCheckpoints(long time, String topic, OffsetRange[] ranges) {
        String rangeJson = new Gson().toJson(ranges);
        String key = getFileKey(""+time, topic);
        LOG.info("Commit key: " + key + " ranges: " + rangeJson);

        writeToLocal(checkpointPrefix + "/" + key, rangeJson);

    }

    public void commitOffsets(long time, String topic) {
        LOG.info("Commit topic: " + topic + " time: " + time);
        writeToLocal(markerPrefix + "/" + topic, String.valueOf(time));
    }

    public OffsetRange[] readOffsets(String id) {
        String jsonString = null;

        // TODO: return null -> offset reset
        try (FileInputStream markerIs = new FileInputStream(root + "/" + markerPrefix + "/" + id)) {
            // compatible parser in S3
            String time = IOUtils.toString(markerIs);
            try (FileInputStream checkIs =
                         new FileInputStream(root + "/" + checkpointPrefix + "/" + getFileKey(time, id))) {
                jsonString = IOUtils.toString(checkIs);
            } catch (IOException e) { return null; }
        } catch (IOException e) { return null; }

        return jsonString == null? null : new Gson().fromJson(jsonString, OffsetRange[].class);
    }

    public Map<TopicPartition, Long> readOffsetsTopicPartition(String topic) {
        String jsonString = null;

        // TODO: return null -> offset reset
        try (FileInputStream markerIs = new FileInputStream(root + "/" + markerPrefix + "/" + topic)) {
            // compatible parser in S3
            String time = IOUtils.toString(markerIs);
            try (FileInputStream checkIs =
                         new FileInputStream(root + "/" + checkpointPrefix + "/" + getFileKey(time, topic))) {
                jsonString = IOUtils.toString(checkIs);
            } catch (IOException e) { return null; }
        } catch (IOException e) { return null; }
        if (jsonString == null){ return null;}
        OffsetRange[] ranges = new Gson().fromJson(jsonString, OffsetRange[].class);

        return toTopicPartitionMap(ranges);
    }

    private static Map<TopicPartition, Long> toTopicPartitionMap(final OffsetRange[] ranges ) {
        return Arrays.stream(ranges).collect(Collectors.toMap(
                offsetRange -> new TopicPartition(offsetRange.topic(), offsetRange.partition()),
                offsetRange -> offsetRange.untilOffset()
        ));
    }

    private static String getFileKey(String time, String id) {
        return ""+ time + "_" + id;
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

    private void writeToLocal(String key, String content){

        int attempts = 0;
        while(attempts++<12) {
            String path = root + "/" + key;
            try (FileOutputStream os = new FileOutputStream(path)) {
                os.write(content.getBytes());
                return; // if success

            }
            catch (IOException e) {
                LOG.warn("Write failed. Will retry after 1sec. e: " + e.toString());
                try {
                    sleep(10000);
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
                }
            }
        }// while end
        LOG.error(new IOException().toString());
    }

}
