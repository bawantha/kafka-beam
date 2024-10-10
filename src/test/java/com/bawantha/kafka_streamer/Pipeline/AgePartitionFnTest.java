package com.bawantha.kafka_streamer.Pipeline;

import org.apache.beam.sdk.values.KV;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AgePartitionFnTest {

    @Test
    public void testPartitionFor_EvenTopic() {
        String evenTopic = "CustomerEVEN";
        String oddTopic = "CustomerODD";
        AgePartitionFn partitionFn = new AgePartitionFn(evenTopic, oddTopic);

        KV<String, String> evenElement = KV.of(evenTopic, "value");
        assertEquals(0, partitionFn.partitionFor(evenElement, 2));
    }

    @Test
    public void testPartitionFor_OddTopic() {
        String evenTopic = "CustomerEVEN";
        String oddTopic = "CustomerODD";
        AgePartitionFn partitionFn = new AgePartitionFn(evenTopic, oddTopic);

        KV<String, String> oddElement = KV.of(oddTopic, "value");
        assertEquals(1, partitionFn.partitionFor(oddElement, 2));
    }

    @Test
    public void testPartitionFor_NullElement() {
        String evenTopic = "CustomerEVEN";
        String oddTopic = "CustomerODD";
        AgePartitionFn partitionFn = new AgePartitionFn(evenTopic, oddTopic);

        assertThrows(IllegalArgumentException.class, () -> partitionFn.partitionFor(null, 2));
    }

    @Test
    public void testPartitionFor_NullKey() {
        String evenTopic = "CustomerEVEN";
        String oddTopic = "CustomerODD";
        AgePartitionFn partitionFn = new AgePartitionFn(evenTopic, oddTopic);

        KV<String, String> elementWithNullKey = KV.of(null, "value");
        assertThrows(IllegalArgumentException.class, () -> partitionFn.partitionFor(elementWithNullKey, 2));
    }
}
