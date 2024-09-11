package io.conduktor.demos;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoCooperativeRebalance {
	private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperativeRebalance.class.getSimpleName());
	
	public static void main(String[] args) {
		log.info("Kafka Consumer With Cooperative Rebalance!!");
		
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "basic-con-demo");
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
//		properties.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "static-member");
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
		final Thread mainThread = Thread.currentThread();
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				log.info("Shutdown Detected, let's exit by calling consumer.wakeup()...");
				consumer.wakeup();
				
				try {
					mainThread.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		
		try {
			consumer.subscribe(Arrays.asList("demo_java"));
			
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
				
				for (ConsumerRecord<String, String> record : records) {
					log.info("Key: " + record.key() + ", Value: " + record.value());
					log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
				}
			}
		} catch (WakeupException e) {
			log.info("Wake Up Exception!");
		} catch (Exception e) {
			log.error("Unexpected Exception!");
		} finally {
			consumer.close();
			log.info("The consumer is now gracefully closed");
		}
	}
}
