package org.tuberlin.benchmark.entities;

import java.util.Random;

public class MonitoringEvent {

    public int patientId;
    public String measurementType;
    public int value;
    public long creationTime;

    public MonitoringEvent(){}
    public MonitoringEvent(int patient, String type) {
      Random rand = new Random();
      this.value = rand.nextInt(100);
      this.creationTime = System.currentTimeMillis();
      this.patientId = patient;
      this.measurementType = type;
    }

    public int getValue() {
        return value;
    }

    public String getType() {
        return measurementType;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public int getID() {
        return patientId;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long time) {
        this.creationTime = time;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof MonitoringEvent) {
            MonitoringEvent monitoringEvent = (MonitoringEvent) obj;
            return monitoringEvent.canEquals(this) &&
                    this.hashCode() == monitoringEvent.hashCode() &&
                    creationTime == monitoringEvent.creationTime;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return this.hashCode();
    }

    public boolean canEquals(Object obj) {
        return obj instanceof MonitoringEvent;
    }
}
