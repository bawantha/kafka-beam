package com.bawantha.kafka_streamer.Pipeline;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BeamOptionsConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaBootstrapServers;

    @Value("${kafka.input-topic}")
    private String inputTopic;

    @Value("${kafka.output-topic-even}")
    private String outputTopicEven;

    @Value("${kafka.output-topic-odd}")
    private String outputTopicOdd;


    @Bean
    public BeamOptions beamOptions() {
        BeamOptions options = PipelineOptionsFactory.create().as(BeamOptions.class);
        options.setKafkaBootstrapServers(kafkaBootstrapServers);
        options.setInputTopic(inputTopic);
        options.setOutputTopicEven(outputTopicEven);
        options.setOutputTopicOdd(outputTopicOdd);
        options.setRunner(DirectRunner.class);
        return options;
    }
}
