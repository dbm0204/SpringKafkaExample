package com.dbm0204.poc.SimpleKafkaProducer.kafkaConsumers;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
public class SimpleKafkaConsumer {
	private static final Logger logger = 
		Logger.getLogger(SimpleKafkaConsumer.class);
	private KafkaConsumer<String, String> kafkaConsumer;
	public SimpleKafkaConsumer(String theTechCheckTopicName, 
			Properties consumerProperties) {
		kafkaConsumer = new KafkaConsumer<>(consumerProperties);
		kafkaConsumer.subscribe(Arrays.asList(theTechCheckTopicName));
	}
    	public void runSingleWorker() {
		while(true) {
			ConsumerRecords<String, String> records = 
				kafkaConsumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				String message = record.value();
				logger.info("Received message: " + message);
				try {
					JSONObject receivedJsonObject = 
						new JSONObject(message);
					logger.info("Index of deserialized JSON object:"+
					receivedJsonObject.getInt("index"));
				} catch (JSONException e) {
					logger.error(e.getMessage());
				}
				{
					Map<TopicPartition, OffsetAndMetadata> 
						commitMessage = new HashMap<>();
					commitMessage.put(new TopicPartition(record.topic(),
								record.partition()),
							new OffsetAndMetadata(record.offset()+ 1));
				    kafkaConsumer.commitSync(commitMessage);
				    logger.info("Offset committed to Kafka.");
			    }
		    }
	    }
    }
}
