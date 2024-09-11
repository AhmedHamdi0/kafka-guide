package io.conduktor.demos;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
	private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());
	
	public static void main(String[] args) {
		log.info("Kafka Producer With Callback!!");
		
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
		
		for (int i = 0; i < 10; i++) {
			ProducerRecord<String, String> producerRecord =
				new ProducerRecord<>("demo_java", "hello world " + i);
			producer.send(producerRecord, new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if (exception == null) {
						log.info("\nReceived Metadata/ \n" +
							"Topic: " + metadata.topic() + "\n" +
							"Partition: " + metadata.partition() + "\n" +
							"Offset: " + metadata.offset() + "\n" +
							"Timestamp: " + metadata.timestamp() + "\n"
						);
					} else {
						log.error("Error While Producing: ", exception);
					}
				}
			});
			
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
		
		producer.flush();
		producer.close();
	}
}
