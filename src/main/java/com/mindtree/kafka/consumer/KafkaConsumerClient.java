package com.mindtree.kafka.consumer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mindtree.kafka.connector.consumer.KafkaConsumerConnector;
public class KafkaConsumerClient {
	private Map<String, Method> annotatedMethods= new HashMap<String, Method>();
	public static void main(String[] args) {
		new KafkaConsumerClient().run();
	}
	// Constructor
	private KafkaConsumerClient() {
	}
	private void run() {
		Logger logger = LoggerFactory.getLogger(KafkaConsumerClient.class.getName());
		// TODO: Change server IP > Move to Property File
		String bootstrapServers = "127.0.0.1:9092";
		// TODO: Change consumergroupname
		String groupID = "group_id";
		// Consume Multiple Topics.
		// TODO: Move to properties
		List<String> topicList= new ArrayList<String>();
		
		ReflectionClient client = new ReflectionClient();
		Set<Method> m = client.getKafkaConsumerAnnonatedMethods();
		KafkaConsumerConnector annotations = null;
		for(Method method : m )  {
			annotations = method.getAnnotation(KafkaConsumerConnector.class);
			System.out.println(annotations);
			annotatedMethods.put(annotations.topicName(),method);
			topicList.add(annotations.topicName()); 
		System.out.println(topicList);
		}
		CountDownLatch latch = new CountDownLatch(1);
		
		//Create consumer runnable
		logger.info("Creating a consumer thread");
		Runnable conumerThread = new ConsumerThread(bootstrapServers, groupID, topicList, latch);
		
		
		Thread kafkaConsumerThread=new Thread(conumerThread);
		// Start the thread
		kafkaConsumerThread.start();
		
		//shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread( () ->{
			logger.info("Caught Shutdown hook");
			((ConsumerThread) conumerThread).shutDown();
			try {
				latch.await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			logger.info("Consumer is shut down");
		}
				
				) );
		
		
		try {
			latch.await();
		} catch (InterruptedException e) {
			logger.error("Exception Occured" + e);
		}finally{
			logger.info("Consumer is closing");
		}
	}
	// Runnable inner class
	public class ConsumerThread implements Runnable {
		private Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());
		private CountDownLatch latch;
		// create a consumer
		private KafkaConsumer<String, String> consumer;
		// TODO: Move to properties
		// TODO: Enable if required in log
		boolean doNeedKeyValuesinLog = false;
		// TODO: Move to properties
		// TODO: Enable if required in log
		boolean doNeedPartitionandOffsetinLog = false;
		// TODO: Move to properties
		Long pollTimeOut = 100L;
		// Constructor
		ConsumerThread(String bootstrapServers, String groupID, List<String> topicList, CountDownLatch countDownlatch) {
			this.latch = countDownlatch;
			Properties properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			/*
			 * AUTO_OFFSET_RESET_CONFIG values are "earliest" = from the beginning of the
			 * topic "latest" = last message from the topic(s) "none" : throws error
			 */
			// Initiate consumer
			consumer = new KafkaConsumer<String, String>(properties);
			// Subscribe consumer to topic
			consumer.subscribe(topicList);
		}
		public void run() {
			// poll for new data
			try {
				while (true) {
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeOut));
									
					for (ConsumerRecord<String, String> record : records) {
						
						Method m = annotatedMethods.get(record.topic());
						System.out.println(m);
						System.out.println(m.getDeclaringClass());
						//m.invoke(m.getDeclaringClass().newInstance(), record.value());
						KafkaConsumerConnector annotations = m.getAnnotation(KafkaConsumerConnector.class);
						//Method m = clzz.getDeclaredMethod("consumeMessage", new Class[] {String.class});
						 
						System.out.println("======topic name======="+  annotations.topicName());
						System.out.println("====message===="+  record.value());
						//System.out.println("");
						
						if (doNeedKeyValuesinLog) {
							// Default Disabled
							logger.info("key:" + record.key() + " value:" + record.value());
						}
						if (doNeedPartitionandOffsetinLog) {
							// Default Disabled
							logger.info("partition:" + record.partition() + " offset:" + record.offset());
						}
					}
				}
			} catch (WakeupException wakeupException) {
				logger.info("Shutdown signal recieved");
			} /*
				 * catch (IllegalAccessException e) { System.out.println("in catch");
				 * e.printStackTrace(); } catch (IllegalArgumentException e) {
				 * System.out.println("in catch"); e.printStackTrace(); } catch
				 * (InvocationTargetException e) { System.out.println("in catch");
				 * e.printStackTrace(); } catch (InstantiationException e) { // TODO
				 * Auto-generated catch block e.printStackTrace(); }
				 */ finally {
				// close consumer
				consumer.close();
				// intimate main code that consumer is closed.
				latch.countDown();
			}
		}
		public void shutDown() {
			// wakeup() is special method to interrupt the poll
			consumer.wakeup();
		}
	}
}
