package com.bawantha.kafka_streamer;
import io.github.cdimascio.dotenv.Dotenv;
import org.apache.beam.sdk.Pipeline;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@SpringBootApplication
public class KafkaStreamerWithApacheBeamApplication {

    public KafkaStreamerWithApacheBeamApplication(Pipeline beamPipeline) {
		beamPipeline.run().waitUntilFinish();
    }

	public static void main(String[] args) {
		SpringApplication.run(KafkaStreamerWithApacheBeamApplication.class, args);
	}

}


