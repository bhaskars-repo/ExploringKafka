/*
 * Name:        MyKafkaConsumerApp2
 * 
 * Description: A simple Kafka message consumer that subscribes messages from a specified topic
 *              with the ability to rewind offset(s)
 * 
 */

package com.polarsparc.Kafka;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Consumer
 *
 */
public class MyKafkaConsumerApp2 {
	public static void main(String[] args) {
		final String PROPERTIES_FILE = "kafka_consumer.properties";
		
    	Logger logger = LoggerFactory.getLogger(MyKafkaConsumerApp2.class);
    	
    	Configuration config = null;
    	
    	Configurations configs = new Configurations();
    	try {
    		config = configs.properties(new File(PROPERTIES_FILE));
    	}
    	catch (Exception ex) {
    		logger.error("Error loading " + PROPERTIES_FILE, ex);
    		System.exit(1);
    	}
    	
    	/* Determine if start from beginning */
    	boolean beginning = false;
    	try {
    		beginning = Boolean.parseBoolean(config.getString("consumer.start.beginning"));
    	}
    	catch (Exception ex) {
    		logger.warn("Invalid <consumer.start.beginning> value - using default false");
    		beginning = false;
    	}
    	
    	logger.info("Consumer start from beginning = " + beginning);
    	
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
        
        Set<Integer> partitionSet = new HashSet<>();
        
        /* Create an instance of a kafka consumer */
        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        
        /* Initialize the list of topic(s) to subscribe to */
        List<String> topics = new ArrayList<>();
        topics.add(config.getString("consumer.topic"));
        
        logger.info("Subscribed topics: " + topics);
        
        /* Subscribe to the desired topics with callback */
        consumer.subscribe(topics, new ConsumerRebalanceListener() {
			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				partitions.forEach(par -> {
					logger.info("REVOKED -> Topic: " + par.topic() + ", Partition: " + par.partition());
					
					partitionSet.remove(par.partition());
				});
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				partitions.forEach(par -> {
					logger.info("ASSIGNED -> Topic: " + par.topic() + ", Partition: " + par.partition());
					
					partitionSet.add(par.partition());
				});
			}
        });
        
        try {
        	boolean once = true;
        	
        	for (int cnt = 0; cnt < 10; cnt++) {
        		/* Poll for a set of ConsumerRecord instances */
        		ConsumerRecords<String, String> records = consumer.poll(1000);
        		
        		if (beginning) {
	        		if (once) {
	        			String topic = config.getString("consumer.topic");
	        			
	        			partitionSet.forEach(par -> {
		        	        /* Set the topic partition offset to beginning */
		        	        consumer.seek(new TopicPartition(topic, par), 0);
	        			});
	        	        
	        			once = false;
	        		}
        		}
        		
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
