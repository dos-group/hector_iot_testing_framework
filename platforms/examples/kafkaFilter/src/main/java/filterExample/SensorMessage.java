package filterExample;

import java.util.Random;

public class SensorMessage {

    public int value;
    public long timestamp;

    public SensorMessage(){
        Random random = new Random();
        this.value = random.nextInt(100);
        this.timestamp = System.currentTimeMillis();
    }

    public int getValue(){
        return value;
    }

    public void setValue(int value){
        this.value = value;
    }

    public long getTimestamp(){
        return timestamp;
    }

    public void setTimestamp(long value){
        this.timestamp = value;
    }
}
