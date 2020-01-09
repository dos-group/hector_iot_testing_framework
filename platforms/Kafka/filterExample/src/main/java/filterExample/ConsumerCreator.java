package filterExample;

import java.util.Collections;
import java.util.Properties;

//import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import filterExample.KafkaConstants;

public class ConsumerCreator {
	public static KafkaConsumer<Integer, SensorMessage> createConsumer(String broker) {
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConstants.GROUP_ID_CONFIG);
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, KafkaConstants.MAX_POLL_RECORDS);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaConstants.OFFSET_RESET_EARLIER);
		final IntegerDeserializer keyDeserializer = new IntegerDeserializer();
		final SensorMessageDeserializer valueDeserializer = new SensorMessageDeserializer();
		final KafkaConsumer<Integer, SensorMessage> consumer = new KafkaConsumer<>(props, keyDeserializer, valueDeserializer);
		consumer.subscribe(Collections.singletonList(KafkaConstants.TOPIC_OUT));
		return consumer;
	}
}
