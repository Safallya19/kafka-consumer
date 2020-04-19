package com.mindtree.kafka.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = "com.mindtree.kafka.*")
public class SpringBootMindtreeKafkaConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringBootMindtreeKafkaConsumerApplication.class, args);
	}

}
