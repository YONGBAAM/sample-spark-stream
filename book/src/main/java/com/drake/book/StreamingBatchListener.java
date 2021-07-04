package com.drake.book;

import lombok.Builder;
import org.apache.spark.streaming.scheduler.*;

import java.util.List;


public class StreamingBatchListener implements StreamingListener {

    @Builder
    private StreamingBatchListener(KafkaOffsetManager kafkaOffsetManager, List<String> dataSourceIds) {
        this.kafkaOffsetManager = kafkaOffsetManager;
        this.dataSourceIds = dataSourceIds;
    }

    private final KafkaOffsetManager kafkaOffsetManager;
    private final List<String> dataSourceIds;

    @Override
    public void onBatchCompleted(StreamingListenerBatchCompleted streamingListenerBatchCompleted) {
        // finish write to hbase : mark time
        dataSourceIds.forEach(id ->
                kafkaOffsetManager.commitOffsets(
                        streamingListenerBatchCompleted.batchInfo().batchTime().milliseconds(), id));

    }

    @Override
    public void onStreamingStarted(StreamingListenerStreamingStarted streamingListenerStreamingStarted) {

    }

    @Override
    public void onReceiverStarted(StreamingListenerReceiverStarted streamingListenerReceiverStarted) {

    }

    @Override
    public void onReceiverError(StreamingListenerReceiverError streamingListenerReceiverError) {

    }

    @Override
    public void onReceiverStopped(StreamingListenerReceiverStopped streamingListenerReceiverStopped) {

    }

    @Override
    public void onBatchSubmitted(StreamingListenerBatchSubmitted streamingListenerBatchSubmitted) {

    }

    @Override
    public void onBatchStarted(StreamingListenerBatchStarted streamingListenerBatchStarted) {

    }



    @Override
    public void onOutputOperationStarted(StreamingListenerOutputOperationStarted streamingListenerOutputOperationStarted) {

    }

    @Override
    public void onOutputOperationCompleted(StreamingListenerOutputOperationCompleted streamingListenerOutputOperationCompleted) {

    }
}
