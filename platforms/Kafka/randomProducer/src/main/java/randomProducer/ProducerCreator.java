package randomProducer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;

public class ProducerCreator {
	public static Producer<Integer, SensorMessage> createProducer(String clientID, String broker) {
		final IntegerSerializer keySerializer = new IntegerSerializer();
		final SensorMessageSerializer valueSerializer = new SensorMessageSerializer();
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, clientID);
		return new KafkaProducer<>(props, keySerializer, valueSerializer);
	}
}
