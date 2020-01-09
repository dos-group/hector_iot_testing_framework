package randomProducer;

import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SensorMessageSerializer implements Serializer<SensorMessage> {

    public SensorMessageSerializer(){
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, SensorMessage data) {
      byte[] retVal = null;
      ObjectMapper objectMapper = new ObjectMapper();
      try {
        retVal = objectMapper.writeValueAsBytes(data);
      } catch (Exception e) {
        e.printStackTrace();
      }
      return retVal;
    }

    @Override
    public void close() {
    }
}
