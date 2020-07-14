package com.mindtree.kafka.listener;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import com.mindtree.kafka.model.Employee;

@Component
public class ListnerKafka {
 
	//@KafkaListener(topics="KAFKA_MINDTREE", groupId= "group_id")
	public void consume(String topic,String message) {
		ContainerProperties containerProperties = new ContainerProperties(topic);
		containerProperties.setMessageListener(new MyMessageListener());
		   ConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProperties());
		    KafkaMessageListenerContainer<String, String> listenerContainer = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
		    listenerContainer.setAutoStartup(false);
		System.out.println("consumed message from kafka_mindtree: "+message);
	}
	
	private Map<String, Object> consumerProperties(){
	    Map<String, Object> props = new HashMap<>();
	    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
	    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
	    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
	    props.put(ConsumerConfig.GROUP_ID_CONFIG, "group_id");
	    return props;
	}
	
	/*
	 * //@KafkaListener(topics="KAFKA_MINDTREE_DEMO", groupId= "group_id") public
	 * void consumeTwo(String message) {
	 * System.out.println("consumed message from kafka_mindtree_demo: "+message); }
	 * 
	 * //@KafkaListener(topics="KAFKA_MINDTREE_JSON", groupId= "group_json",
	 * containerFactory = "userKafkaConcurrentKafkaListenerContainerFactory") public
	 * void consumeJson(Employee emp) { System.out.println("consumed message:"+
	 * emp); }
	 */
	
	static class MyMessageListener implements MessageListener<String, String> 
	{
	@Override
	public void onMessage(ConsumerRecord<String, String> data) {
		System.out.println("message kafka :"+ data.value());		
	}
	}
}
