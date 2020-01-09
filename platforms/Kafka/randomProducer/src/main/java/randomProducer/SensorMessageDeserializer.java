package randomProducer;

import org.apache.kafka.common.serialization.Deserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;

public class SensorMessageDeserializer implements Deserializer<SensorMessage> {

    public SensorMessageDeserializer(){
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public SensorMessage deserialize(String topic, byte[] data) {
      ObjectMapper objectMapper = new ObjectMapper();
      try {
            return objectMapper.readValue(new String(data, "UTF-8"), SensorMessage.class);
        } catch (Exception e) {
            System.out.println("Error while deserializing: " + e);
            return null;
        }
    }

    @Override
    public void close() {
    }
}
