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
    // Json으로 받기

    public void checkOffsets(String time, String id, OffsetRange[] ranges) {
        String rangeJson = new Gson().toJson(ranges);
        String key = getFileKey(time, id);
        LOG.info("Commit key: " + key + " ranges: " + rangeJson);

        writeToLocal(checkpointPrefix + "/" + key, rangeJson);

    }

    public void markOffsets(String time, String id) {
        LOG.info("Commit id: " + id + " time: " + time);
        writeToLocal(markerPrefix + "/" + id, time);
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

    private static String getFileKey(String time, String id) {
        return time + "_" + id;
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
