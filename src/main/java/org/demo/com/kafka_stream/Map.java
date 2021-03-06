package org.demo.com.kafka_stream;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;


public class Map {


	public static void main(String[] args) throws Exception {
			Properties props = new Properties();
	        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "group");
	        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
	        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
	        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
	        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());

	        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
	        props.put("auto.offset.reset", "earliest");

	        KStreamBuilder builder = new KStreamBuilder();

	        KStream<String, byte[]> source = builder.stream("test-topic2");
	        
	        
	        source.map(new KeyValueMapper<String, byte[], KeyValue<String,String>>() {
				@Override
				public KeyValue<String, String> apply(String key, byte[] value) {
					System.out.println("key:"+key+ " value: "+new String(value));
					return new KeyValue<String, String>(key, new String(value));

				}
			});
	        
	       /** source.mapValues(new ValueMapper<byte[], String>() {

				@Override
				public String apply(byte[] value) {
					System.out.println("value: "+new String(value));
					return  new String(value);
				}
			});**/
	        		
	        source.print();
	        
	        KafkaStreams streams = new KafkaStreams(builder, props);
	        streams.start();

	      /**  // usually the stream application would be running forever,
	        // in this example we just let it run for some time and stop since the input data is finite.
	        Thread.sleep(60000L);

	        streams.close();**/
	    }
}
