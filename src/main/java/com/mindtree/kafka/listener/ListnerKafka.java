package com.mindtree.kafka.listener;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.mindtree.kafka.model.Employee;

@Service
public class ListnerKafka {

	@KafkaListener(topics="KAFKA_MINDTREE", groupId= "group_id")
	public void consume(String message) {
		System.out.println("consumed message:"+ message);
	}
	
	@KafkaListener(topics="KAFKA_MINDTREE_JSON", groupId= "group_json", containerFactory = "userKafkaConcurrentKafkaListenerContainerFactory")
	public void consumeJson(Employee emp) {
		System.out.println("consumed message:"+ emp);
	}
}
