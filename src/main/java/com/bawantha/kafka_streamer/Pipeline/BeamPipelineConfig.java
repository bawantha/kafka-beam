package com.bawantha.kafka_streamer.Pipeline;

import com.google.common.annotations.VisibleForTesting;
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

@Configuration
public class BeamPipelineConfig {

    private static final String REGEX_NON_PRINTABLE = "[^\\x20-\\x7E]";

    @Autowired
    BeamOptionsConfig options;

    @Bean
    public Pipeline pipeline() {
        Pipeline p = Pipeline.create(options.beamOptions());

        // Reading from Kafka
        PCollectionList<KV<String, String>> partitions = p.apply(KafkaIO.<String, String>read()
                        .withBootstrapServers(options.beamOptions().getKafkaBootstrapServers())
                        .withTopic(options.beamOptions().getInputTopic())
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                        .withConsumerConfigUpdates(options.kafkaConfluentConfig())
                        .withoutMetadata())
                .apply("ExtractYear", getDob())
                .apply("PartitionTopics", Partition.of(2, new AgePartitionFn(
                        options.beamOptions().getOutputTopicEven(),
                        options.beamOptions().getOutputTopicOdd()
                )));

        // Publish to topics based on partitions
        publishToKafka(partitions);

        p.run().waitUntilFinish();
        return p;
    }

    private void publishToKafka(PCollectionList<KV<String, String>> partitions) {
        if (partitions.size() < 2) {
            throw new IllegalArgumentException("Expected two partitions, but got: " + partitions.size());
        }

        partitions.get(0) // Even topic
                .apply("PublishToEvenTopic", KafkaIO.<String, String>write()
                        .withBootstrapServers(options.beamOptions().getKafkaBootstrapServers())
                        .withKeySerializer(StringSerializer.class)
                        .withValueSerializer(StringSerializer.class)
                        .withTopic(options.beamOptions().getOutputTopicEven())
                        .withProducerConfigUpdates(options.kafkaConfluentConfig()));

        partitions.get(1) // Odd topic
                .apply("PublishToOddTopic", KafkaIO.<String, String>write()
                        .withBootstrapServers(options.beamOptions().getKafkaBootstrapServers())
                        .withKeySerializer(StringSerializer.class)
                        .withValueSerializer(StringSerializer.class)
                        .withTopic(options.beamOptions().getOutputTopicOdd())
                        .withProducerConfigUpdates(options.kafkaConfluentConfig()));
    }


    public static ParDo.SingleOutput<KV<String, String>, KV<String, String>> getDob() {
        return ParDo.of(new DoFn<KV<String, String>, KV<String, String>>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                KV<String, String> element = c.element();
                if (element == null || element.getValue() == null) {
                    return;
                }

                String value = element.getValue().replaceAll(REGEX_NON_PRINTABLE, "");
                JsonObject jsonObject = JsonParser.parseString(value).getAsJsonObject();

                if (jsonObject.has("dateOfBirth")) {
                    String dobStr = jsonObject.get("dateOfBirth").getAsString();
                    LocalDate dob = LocalDate.parse(dobStr, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                    int year = dob.getYear();

                    String topic = (year % 2 == 0) ? c.getPipelineOptions().as(BeamOptions.class).getOutputTopicEven()
                            : c.getPipelineOptions().as(BeamOptions.class).getOutputTopicOdd();
                    c.output(KV.of(topic, value));
                }
            }
        });
    }

}

