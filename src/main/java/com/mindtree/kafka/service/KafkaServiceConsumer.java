package com.mindtree.kafka.service;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.mindtree.kafka.connector.consumer.KafkaConsmerPro;
import com.mindtree.kafka.connector.consumer.KafkaConsumerConnector;
import com.mindtree.kafka.listener.ListnerKafka;

@Service
public class KafkaServiceConsumer {
	
	@Autowired
	KafkaConsmerPro pro;
	
	@Autowired
	ListnerKafka kl;
 
	@KafkaConsumerConnector(topicName="KAFKA_MINDTREE",  message="message from kafka mindtree")
	public String  consumeMessage(String topic) throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		
		String message= null; 
		 message=	consumeKafkaMindtree(topic);
		 System.out.println("message out from consume message:"+message);
		 return message;
	}
	
	
	
	/*
	 * @KafkaListener(id = "simple-consumer", topics = "KAFKA_MINDTREE") public void
	 * consumeMessage(String message) { System.out.println("Got message: " +
	 * message); }
	 */
	 
	@KafkaConsumerConnector(topicName="KAFKA_MINDTREE",  message="message from kafka mindtree") 
	public String  consumeKafkaMindtree(String message) throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException { 
		String msg= pro.process(this, message,kl);
		System.out.println("message out:"+msg);
		return msg;
	}
	
	/*
	 * @KafkaConsumerConnector(topicName="KAFKA_MINDTREE_DEMO",
	 * message="message from kafka mindtree demo") public void
	 * consumeKafkaMindtreeDemo(String topic) throws NoSuchMethodException,
	 * SecurityException, IllegalAccessException, IllegalArgumentException,
	 * InvocationTargetException { pro.processTwo(this, topic,kl); }
	 */
	
}
