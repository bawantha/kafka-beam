package com.bawantha.kafka_streamer.Pipeline;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
public class BeamOptionsConfig {

    private static final String SECURITY_PROTOCOL = "SASL_SSL";
    private static final String SASL_MECHANISM = "PLAIN";

    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaBootstrapServers;

    @Value("${kafka.input-topic}")
    private String inputTopic;

    @Value("${kafka.output-topic-even}")
    private String outputTopicEven;

    @Value("${kafka.output-topic-odd}")
    private String outputTopicOdd;

    @Value("${kafka.api-key}")
    private String apiKey;

    @Value("${kafka.secret-key}")
    private String secretKey;

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

    @Bean
    public Map<String, Object> kafkaConfluentConfig() {
        return Map.of(
                "security.protocol", SECURITY_PROTOCOL,
                "sasl.jaas.config", String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username='%s' password='%s';", apiKey, secretKey),
                "sasl.mechanism", SASL_MECHANISM
        );
    }

}
