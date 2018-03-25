/*
 * Name:        MyKafkaConsumerApp
 * 
 * Description: A simple Kafka message consumer that subscribes messages from a specified topic
 * 
 */

package com.polarsparc.Kafka;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Consumer
 *
 */
public class MyKafkaConsumerApp {
	public static void main(String[] args) {
		final String PROPERTIES_FILE = "kafka_consumer.properties";
		
    	Logger logger = LoggerFactory.getLogger(MyKafkaConsumerApp.class);
    	
    	Configuration config = null;
    	
    	Configurations configs = new Configurations();
    	try {
    		config = configs.properties(new File(PROPERTIES_FILE));
    	}
    	catch (Exception ex) {
    		logger.error("Error loading " + PROPERTIES_FILE, ex);
    		System.exit(1);
    	}
    	
    	/* Initialize the properties from kafka_consumer.properties */
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        		config.getString(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        		config.getString(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        		config.getString(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
        		config.getString(ConsumerConfig.GROUP_ID_CONFIG));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        		config.getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        
        /* Create an instance of a kafka consumer */
        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        
        /* Initialize the list of topic(s) to subscribe to */
        List<String> topics = new ArrayList<>();
        topics.add(config.getString("consumer.topic"));
        
        /* Subscribe to the desired topics */
        consumer.subscribe(topics);
        
        try {
        	for (int cnt = 0; cnt < 10; cnt++) {
        		/* Poll for a set of ConsumerRecord instances */
        		ConsumerRecords<String, String> records = consumer.poll(1000);
        		
        		/* Display the messages */
        		records.forEach(rec -> {
        			logger.info("Partition: " + rec.partition() + ", Offset: " + rec.offset() + ", Value: " + rec.value());
        		});
        	}
        }
    	catch (Exception ex) {
    		logger.error("Error receiving message(s)", ex);
    	}
        
        consumer.close();
	}
}
