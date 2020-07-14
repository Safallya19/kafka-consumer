package com.mindtree.kafka.connector.consumer;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;

@Retention(RUNTIME)
public @interface KafkaConsumerConnector {
    String topicName() default "topic"; 
    String message()  default "message";
}
