package com.mindtree.kafka.connector.consumer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.mindtree.kafka.listener.ListnerKafka;

@Component
public class KafkaConsmerPro {

	/*
	 * @Autowired ListnerKafka kl;
	 */

	  
	// @KafkaListener(id = "simple-consumer", topics = "KAFKA_MINDTREE")
	public String  process(Object c, String message,ListnerKafka kl) throws NoSuchMethodException, SecurityException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException {
		Class<?> clzz = c.getClass();
		Method m = clzz.getDeclaredMethod("consumeMessage", new Class[] {String.class});
		//m.invoke(c);
		KafkaConsumerConnector annotations = m.getAnnotation(KafkaConsumerConnector.class);

		String topicName = annotations.topicName(); 
		kl.consume(topicName, message);
		 return message;
		
	}

	public void processTwo(Object c, String topic,ListnerKafka kl) throws NoSuchMethodException, SecurityException,
			IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		Class<?> clzz = c.getClass();
		Method m = clzz.getDeclaredMethod("consumeKafkaMindtreeDemo", new Class[] {String.class});
		//m.invoke(c);
		KafkaConsumerConnector annotations = m.getAnnotation(KafkaConsumerConnector.class);

		String topicName = annotations.topicName();
		String message = annotations.message();
		// String topicName= annotations.topicName(); String message =
		annotations.message();
		kl.consume(topicName,message);
	}

}
