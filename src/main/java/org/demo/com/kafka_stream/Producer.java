package org.demo.com.kafka_stream;


import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class Producer {

	public static void main(String[] args) throws Exception {
		String server = "localhost:9092";
		KafkaProducer<String, byte[]> kafkaProducer;
		Properties props = new Properties();
		props.put("bootstrap.servers", server);
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.sie", 16314);
		props.put("linger.ms", 1);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		
		kafkaProducer = new KafkaProducer<>(props);
		System.out.println("Connected to kafka !");
		for(int i = 500; i < 600; i++){
			String message = "message"+i;
			//kafkaProducer.send(new ProducerRecord<String, byte[]>("test-topic", message, message.getBytes()));
			
			kafkaProducer.send(new ProducerRecord<String, byte[]>("test-topic2", 5, message, message.getBytes()));
			System.out.println("Puhsed to Kafka : "+message);
		}
		kafkaProducer.close();
		
	}
}
