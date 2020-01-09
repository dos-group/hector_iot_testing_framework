package org.tuberlin.benchmark.entities;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class MonitoringEventSource extends RichParallelSourceFunction<MonitoringEvent> {

    private boolean running = true;

    private final int rate;
    public int id;
    public String type;

    public MonitoringEventSource(int id, int rate, String type) {
        this.rate = rate;
        this.id = id;
        this.type = type;
    }

    @Override
    public void open(Configuration configuration) {
        int numberTasks = getRuntimeContext().getNumberOfParallelSubtasks();
        int index = getRuntimeContext().getIndexOfThisSubtask();
    }

    public void run(SourceContext<MonitoringEvent> sourceContext) throws Exception {
        while (running) {
            MonitoringEvent monitoringEvent = new MonitoringEvent(id, type);
            sourceContext.collect(monitoringEvent);

            if(rate < Integer.MAX_VALUE){
                Thread.sleep(1000 / rate); //Rate is per second
            }

        }
    }

    public void cancel() {
        running = false;
    }
}
