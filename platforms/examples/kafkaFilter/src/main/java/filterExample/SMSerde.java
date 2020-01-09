package filterExample;

import org.apache.kafka.common.serialization.Serde;
import java.util.Map;

public class SMSerde implements Serde<SensorMessage> {
    final private SensorMessageSerializer serializer;
    final private SensorMessageDeserializer deserializer;

    public SMSerde() {
        this.serializer = new SensorMessageSerializer();
        this.deserializer = new SensorMessageDeserializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        serializer.close();
        deserializer.close();
    }

    @Override
    public SensorMessageSerializer serializer() {
        return serializer;
    }

    @Override
    public SensorMessageDeserializer deserializer() {
        return deserializer;
    }
}
