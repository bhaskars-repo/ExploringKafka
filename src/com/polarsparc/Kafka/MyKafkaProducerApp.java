/*
 * Name:        MyKafkaProducerApp
 * 
 * Description: A simple Kafka message producer that publishes messages to a specified topic
 * 
 */

package com.polarsparc.Kafka;

import java.io.File;
import java.util.Properties;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Producer
 *
 */
public class MyKafkaProducerApp 
{
    public static void main( String[] args )
    {
    	if (args.length != 2) {
    		System.out.printf("Usage: java %s <count> <prefix>\n", MyKafkaProducerApp.class.getName());
    		System.exit(1);
    	}
    	
    	final String PROPERTIES_FILE = "kafka_producer.properties";
    	
    	Logger logger = LoggerFactory.getLogger(MyKafkaProducerApp.class);
    	
    	/* Determine the count of message to publish to Kafka */
    	int count = 0;
    	try {
    		count = Integer.parseInt(args[0]);
    		if (count <= 0) {
    			count = 10;
    		}
    	}
    	catch (Exception ex) {
    		logger.warn("Invalid <count> value - using default 10");
    		count = 10;
    	}
    	
    	logger.info("Message count = " + count);
    	
    	Configuration config = null;
    	
    	Configurations configs = new Configurations();
    	try {
    		config = configs.properties(new File(PROPERTIES_FILE));
    	}
    	catch (Exception ex) {
    		logger.error("Error loading " + PROPERTIES_FILE, ex);
    		System.exit(1);
    	}
    	
    	/* Initialize the properties from kafka_producer.properties */
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        		config.getString(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        		config.getString(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        		config.getString(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        props.put(ProducerConfig.ACKS_CONFIG,
        		config.getString(ProducerConfig.ACKS_CONFIG));
        
        /* Create an instance of a kafka producer */
        Producer<String, String> producer = new KafkaProducer<>(props);
        
        for (int i = 0; i < count; i++) {
        	/* Create an instance of ProducerRecord using the Topic name, the key, and the message content */
        	ProducerRecord<String, String> record = new ProducerRecord<>(config.getString("producer.topic"), 
        			String.valueOf(i), args[1] + " - " + (i+1));
        	
        	/* Send the data record to kafka */
        	try {
        		producer.send(record).get();
        	}
        	catch (Exception ex) {
        		logger.error("Error sending message ["+i+"]", ex);
        	}
        }
        
        /* Close the kafka producer */
        producer.close();
    }
}
