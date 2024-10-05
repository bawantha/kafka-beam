package com.bawantha.kafka_streamer.Pipeline;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface BeamOptions extends PipelineOptions {
    @Description("Kafka bootstrap servers, comma-separated")
    String getKafkaBootstrapServers();
    void setKafkaBootstrapServers(String value);

    @Description("Input Kafka topic")
    String getInputTopic();
    void setInputTopic(String value);

    @Description("Output Kafka topic for even ages")
    String getOutputTopicEven();
    void setOutputTopicEven(String value);

    @Description("Output Kafka topic for odd ages")
    String getOutputTopicOdd();
    void setOutputTopicOdd(String value);
}