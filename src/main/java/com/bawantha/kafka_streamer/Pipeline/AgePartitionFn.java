package com.bawantha.kafka_streamer.Pipeline;

import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.KV;

public class AgePartitionFn implements Partition.PartitionFn<KV<String, String>> {
    private final String evenTopic;
    private final String oddTopic;

    public AgePartitionFn(String evenTopic, String oddTopic) {
        this.evenTopic = evenTopic;
        this.oddTopic = oddTopic;
    }

    @Override
    public int partitionFor(KV<String, String> element, int numPartitions) {
        if (element == null || element.getKey() == null) {
            throw new IllegalArgumentException("Element or its key cannot be null");
        }
        return evenTopic.equals(element.getKey()) ? 0 : 1;
    }
}
