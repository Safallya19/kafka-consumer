package com.mindtree.kafka.controller;

import java.lang.reflect.InvocationTargetException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.mindtree.kafka.service.KafkaServiceConsumer;

@RestController
@RequestMapping("/consumer")
public class DemoConsumerController {
	
	@Autowired
	KafkaServiceConsumer kafkaService;

	@GetMapping(value = "/topicName")
	public String consumerTopic(@RequestParam("topic") String topic ) throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		String message=kafkaService.consumeMessage(topic);
		return "Message from Topic :"+ message;
	}
}

