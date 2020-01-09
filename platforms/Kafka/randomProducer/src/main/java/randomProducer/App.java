package randomProducer;

import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import java.net.DatagramSocket;
import java.net.InetAddress;

public class App {

	public static int ip;

	public static void main(String[] args) throws Exception{
		String topic = args[0];
		String broker = args[1];
		int i;
		try(final DatagramSocket socket = new DatagramSocket()){
		  socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
			try{
		  	ip = Integer.parseInt(socket.getLocalAddress().getHostAddress().split("\\.")[3]);
			}catch (NumberFormatException e)
				{
				   ip = 0;
				}
		}
		System.out.println("Starting producer on topic " + topic + " with broker "+ broker);
		runProducer("sensorProducer", topic, broker);
		System.exit(0);
	}

	static void runProducer(String ProducerId, String topic, String broker) {
		Producer<Integer, SensorMessage> producer = ProducerCreator.createProducer(ProducerId, broker);

		while(true) {
			SensorMessage	message = new SensorMessage();
			final ProducerRecord<Integer, SensorMessage> record = new ProducerRecord<Integer, SensorMessage>(topic, ip, message); // Send Number between 0 and 99
			try {
				RecordMetadata metadata = producer.send(record).get();
//				System.out.println("Record sent with value " + record.value() + " to partition " + metadata.partition()
//						+ " with offset " + metadata.offset());
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
