package com.bawantha.kafka_streamer.Pipeline;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Objects;

@Configuration
public class BeamPipelineConfig {

    @Autowired
    BeamOptionsConfig options;

    @Bean
    Pipeline pipeline() {
        Pipeline p = Pipeline.create(options.beamOptions());

        // Reading from Kafka
        PCollectionList<KV<String, String>> partitions = p.apply(KafkaIO.<String, String>read()
                        .withBootstrapServers(options.beamOptions().getKafkaBootstrapServers())
                        .withTopic(options.beamOptions().getInputTopic())
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                        .withConsumerConfigUpdates(getKafkaConsumerConfig())
                        .withoutMetadata())  // Avoid metadata overhead if not needed
                .apply("ExtractYear", getDob())
                .apply("PartitionTopics", Partition.of(2, new PartitionFn()));

        // Publish to topics based on partitions
        publishToKafka(partitions);

        p.run().waitUntilFinish();
        return p;
    }

    private Map<String, Object> getKafkaConsumerConfig() {
        return ImmutableMap.of(
                "security.protocol", "SASL_SSL",
                "sasl.jaas.config", String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username='%s' password='%s';",
                        System.getenv("KAFKA_USERNAME"), System.getenv("KAFKA_PASSWORD")),
                "sasl.mechanism", "PLAIN"
        );
    }

    private void publishToKafka(PCollectionList<KV<String, String>> partitions) {
        partitions.get(0)  // Even topic
                .apply("PublishToEvenTopic", KafkaIO.<String, String>write()
                        .withBootstrapServers(options.beamOptions().getKafkaBootstrapServers())
                        .withKeySerializer(StringSerializer.class)
                        .withValueSerializer(StringSerializer.class)
                        .withTopic(options.beamOptions().getOutputTopicEven())
                        .withProducerConfigUpdates(getKafkaProducerConfig()));

        partitions.get(1)  // Odd topic
                .apply("PublishToOddTopic", KafkaIO.<String, String>write()
                        .withBootstrapServers(options.beamOptions().getKafkaBootstrapServers())
                        .withKeySerializer(StringSerializer.class)
                        .withValueSerializer(StringSerializer.class)
                        .withTopic(options.beamOptions().getOutputTopicOdd())
                        .withProducerConfigUpdates(getKafkaProducerConfig()));
    }

    private Map<String, Object> getKafkaProducerConfig() {
        return ImmutableMap.of(
                "security.protocol", "SASL_SSL",
                "sasl.jaas.config", String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username='%s' password='%s';",
                        System.getenv("KAFKA_USERNAME"), System.getenv("KAFKA_PASSWORD")),
                "sasl.mechanism", "PLAIN"
        );
    }

    private static ParDo.SingleOutput<KV<String, String>, KV<String, String>> getDob() {
        return ParDo.of(new DoFn<KV<String, String>, KV<String, String>>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String value = Objects.requireNonNull(c.element()).getValue();
                assert value != null;
                value = value.replaceAll("[^\\x20-\\x7E]", "");
                JsonObject jsonObject = JsonParser.parseString(value).getAsJsonObject();
                String dobStr = jsonObject.get("dateOfBirth").getAsString();
                LocalDate dob = LocalDate.parse(dobStr, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                int year = dob.getYear();
                // Even year or Odd year routing
                String topic = (year % 2 == 0) ? "CustomerEVEN" : "CustomerODD";
                c.output(KV.of(topic, value));
            }
        });
    }

    private static class PartitionFn implements org.apache.beam.sdk.transforms.Partition.PartitionFn<KV<String, String>> {
        @Override
        public int partitionFor(KV<String, String> element, int numPartitions) {
            assert element != null;
            String topicKey = element.getKey();
            return "EvenAgeTopic".equals(topicKey) ? 0 : 1; // Send to respective partition
        }
    }
}
