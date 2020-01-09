package filterExample;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.apache.kafka.streams.KeyValue;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.lang.Math;

import filterExample.KafkaConstants;
import filterExample.SMSerde;

public class StreamFilter {

	public static void runStreamFilter(String broker) throws Exception {
			final SensorMessageSerializer serializer = new SensorMessageSerializer();
			final SensorMessageDeserializer deserializer = new SensorMessageDeserializer();
			SMSerde valueSerde = new SMSerde();
	    Properties props = new Properties();
	    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-filter");
	    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
	    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
	    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, valueSerde.getClass());

	    final StreamsBuilder builder = new StreamsBuilder();

	    final KStream<Integer, SensorMessage> input = builder.stream(KafkaConstants.TOPIC_IN, Consumed.with(Serdes.Integer(), Serdes.serdeFrom(serializer, deserializer)));
	    final KStream<Integer, SensorMessage> output = input.filter((k,v) -> v.getValue() > 19)
																							.mapValues(value -> cubicMapper(value))
																							.groupByKey(Grouped.with(Serdes.Integer(), Serdes.serdeFrom(serializer, deserializer)))
																							//.windowedBy(TimeWindows.of(Duration.ofMillis(10)).grace(Duration.ofMillis(0)))
																							.reduce((aggValue, newValue) -> avgReducer(aggValue, newValue))

																							//.suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))
																							.toStream()
																							.mapValues(value -> latencyMapper(value));
																							//.map((key, value) -> KeyValue.pair(key.key(), value));



	    output.to(KafkaConstants.TOPIC_OUT);
	   	final Topology topology = builder.build();

	    final KafkaStreams streams = new KafkaStreams(topology, props);
	    final CountDownLatch latch = new CountDownLatch(1);

	    // attach shutdown handler to catch control-c
	    Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
	        @Override
	        public void run() {
	            streams.close();
	            latch.countDown();
	        }
	    });

	    try {
	        streams.start();
	    } catch (Throwable e) {
	    	System.out.println("Exited Streams App with Error(s)!");
	        System.exit(1);
	    }
	}

		public static SensorMessage avgReducer(SensorMessage aggValue, SensorMessage newValue){
				SensorMessage result = new SensorMessage();
				result.setValue((aggValue.getValue() + newValue.getValue())/2);
				result.setTimestamp((aggValue.getTimestamp() + newValue.getTimestamp())/2);
				return result;
		}

		public static SensorMessage latencyMapper(SensorMessage value){
				value.setTimestamp(System.currentTimeMillis() - value.getTimestamp());
				return value;
		}

		public static SensorMessage cubicMapper(SensorMessage value){
				value.setValue((int)Math.pow(value.getValue(),3));
				return value;
		}
}
