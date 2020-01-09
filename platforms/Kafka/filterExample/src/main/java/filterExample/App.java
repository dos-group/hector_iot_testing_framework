package filterExample;

import java.util.concurrent.ExecutionException;
import java.util.Random;
import java.time.Duration;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

// import filterExample.KafkaConstants;
// import filterExample.ConsumerCreator;
// import filterExample.ProducerCreator;
// import filterExample.StreamFilter;

public class App {

	public static void main(String[] args) throws Exception{
		String broker = args[0];
		StreamFilter.runStreamFilter(broker);
		System.out.println("Stream App started!");
		runConsumer(broker);
		System.exit(0);
	}

	static void runConsumer(String broker) {
		KafkaConsumer<Integer, SensorMessage> consumer = ConsumerCreator.createConsumer(broker);

		int noMessageToFetch = 0;
		Duration timeout = Duration.ofSeconds(1);
		while (true) {
			final ConsumerRecords<Integer, SensorMessage> consumerRecords = consumer.poll(timeout);
			if (consumerRecords.count() == 0) {
				noMessageToFetch++;
				if (noMessageToFetch > KafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
					break;
				else
					noMessageToFetch = 0;
					continue;
			}

			consumerRecords.forEach(record -> {
				System.out.println(record.key() + ", " + record.value().getTimestamp());
			});
			consumer.commitAsync();
		}
		consumer.close();
	}

	static void runProducer(String ProducerId, String broker) {
		Producer<Long, Long> producer = ProducerCreator.createProducer(ProducerId,broker);
		Random rand = new Random();

		for (int index = 0; index < KafkaConstants.MESSAGE_COUNT; index++) {
			long n = rand.nextInt(100);
			final ProducerRecord<Long, Long> record = new ProducerRecord<Long, Long>(KafkaConstants.TOPIC_IN, (long) n); // Send Number between 0 and 99
			try {
				RecordMetadata metadata = producer.send(record).get();
				System.out.println("Record sent with value " + record.value() + " to partition " + metadata.partition()
						+ " with offset " + metadata.offset());
			} catch (ExecutionException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
			} catch (InterruptedException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
			}
		}
	}

}
