package com.mindtree.kafka.consumer;

import java.lang.reflect.Method;
import java.util.Set;

import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.scanners.MethodParameterScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import com.mindtree.kafka.connector.consumer.KafkaConsumerConnector;
public class ReflectionClient {

	Reflections reflections = new Reflections(new ConfigurationBuilder()
			.setUrls(ClasspathHelper.forPackage("com.mindtree.kafka.service"))
			.setScanners(new TypeAnnotationsScanner(), new MethodParameterScanner(), new MethodAnnotationsScanner()));
	
	public Set<Method> getKafkaConsumerAnnonatedMethods() {
		
	    return reflections.getMethodsAnnotatedWith(KafkaConsumerConnector.class);
	}
	
}
