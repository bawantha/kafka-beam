package com.bawantha.kafka_streamer.Pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class BeamPipelineTest {

    private TestPipeline pipeline;

    @Before
    public void setup() {
        pipeline = TestPipeline.create();
    }

    @Test
    public void testPipelinePartition() {
        // Define sample input data
        List<KV<String, String>> input = Arrays.asList(
                KV.of("key1", "{\"dateOfBirth\":\"1980-01-01\"}"),
                KV.of("key2", "{\"dateOfBirth\":\"1991-05-05\"}"),
                KV.of("key3", "{\"dateOfBirth\":\"1988-07-07\"}"),
                KV.of("key4", "{\"dateOfBirth\":\"1995-09-09\"}"),
                KV.of("key5", null)  // Adding a null value for testing
        );

        // Expected output for even and odd topics
        List<KV<String, String>> evenExpected = Arrays.asList(
                KV.of("evenTopic", "{\"dateOfBirth\":\"1980-01-01\"}"),
                KV.of("evenTopic", "{\"dateOfBirth\":\"1988-07-07\"}")
        );

        List<KV<String, String>> oddExpected = Arrays.asList(
                KV.of("oddTopic", "{\"dateOfBirth\":\"1991-05-05\"}"),
                KV.of("oddTopic", "{\"dateOfBirth\":\"1995-09-09\"}")
        );

        // Create input PCollection, filter out null values
        PCollection<KV<String, String>> inputCollection = pipeline
                .apply(Create.of(input))
                .apply("FilterNullValues", Filter.by(kv -> kv.getValue() != null));

        // Apply the ExtractYear and Partition transforms
        PCollectionList<KV<String, String>> partitions = inputCollection
                .apply("ExtractYear", BeamPipelineConfig.getDob()) // Make sure this transform is implemented
                .apply("PartitionTopics", Partition.of(2,
                        new AgePartitionFn("evenTopic", "oddTopic"))); // Ensure AgePartitionFn is defined

        // Validate output using PAssert for both partitions
        PAssert.that(partitions.get(0)).containsInAnyOrder(evenExpected);
        PAssert.that(partitions.get(1)).containsInAnyOrder(oddExpected);

        // Run the pipeline
        pipeline.run().waitUntilFinish();
    }
}
