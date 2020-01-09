package org.tuberlin.benchmark;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.tuberlin.benchmark.entities.MonitoringEvent;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.tuberlin.benchmark.entities.MonitoringEventSource;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.api.java.tuple.Tuple3;
import java.lang.Math;

public class IoTBenchmark {

    static StreamExecutionEnvironment env;

    public static void main(String[] args) throws Exception {

        env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.disableOperatorChaining();

        int rate = args.length >= 1 ? Integer.parseInt(args[0]) : Integer.MAX_VALUE; // no limitation for message generator
        String outPath = args[1];

        if (rate == -1){
            rate = Integer.MAX_VALUE;
        }

        DataStream<MonitoringEvent> sensorStream1 = createSourceStream(1, rate, "HeartRate");
        DataStream<MonitoringEvent> sensorStream2 = createSourceStream(1, rate, "OxigenLevel");
        // DataStream<MonitoringEvent> sensorStream3 = createSourceStream(2, rate, "HeartRate");
        // DataStream<MonitoringEvent> sensorStream4 = createSourceStream(2, rate, "OxigenLevel");
        // DataStream<MonitoringEvent> sensorStream5 = createSourceStream(3, rate, "HeartRate");
        // DataStream<MonitoringEvent> sensorStream6 = createSourceStream(3, rate, "OxigenLevel");
        // DataStream<MonitoringEvent> sensorStream7 = createSourceStream(4, rate, "HeartRate");
        // DataStream<MonitoringEvent> sensorStream8 = createSourceStream(4, rate, "OxigenLevel");
        // DataStream<MonitoringEvent> sensorStream9 = createSourceStream(5, rate, "HeartRate");
        // DataStream<MonitoringEvent> sensorStream10 = createSourceStream(5, rate, "OxigenLevel");
        // DataStream<MonitoringEvent> sensorStream11 = createSourceStream(6, rate, "HeartRate");
        // DataStream<MonitoringEvent> sensorStream12 = createSourceStream(6, rate, "OxigenLevel");
        // DataStream<MonitoringEvent> sensorStream13 = createSourceStream(7, rate, "HeartRate");
        // DataStream<MonitoringEvent> sensorStream14 = createSourceStream(7, rate, "OxigenLevel");
        // DataStream<MonitoringEvent> sensorStream15 = createSourceStream(8, rate, "HeartRate");
        // DataStream<MonitoringEvent> sensorStream16 = createSourceStream(8, rate, "OxigenLevel");
        // DataStream<MonitoringEvent> sensorStream17 = createSourceStream(9, rate, "HeartRate");
        // DataStream<MonitoringEvent> sensorStream18 = createSourceStream(9, rate, "OxigenLevel");
        // DataStream<MonitoringEvent> sensorStream19 = createSourceStream(10, rate, "HeartRate");
        // DataStream<MonitoringEvent> sensorStream20 = createSourceStream(10, rate, "OxigenLevel");
        // DataStream<MonitoringEvent> sensorStream21 = createSourceStream(11, rate, "HeartRate");
        // DataStream<MonitoringEvent> sensorStream22 = createSourceStream(11, rate, "OxigenLevel");
        // DataStream<MonitoringEvent> sensorStream23 = createSourceStream(12, rate, "HeartRate");
        // DataStream<MonitoringEvent> sensorStream24 = createSourceStream(12, rate, "OxigenLevel");
        // DataStream<MonitoringEvent> sensorStream25 = createSourceStream(13, rate, "HeartRate");
        // DataStream<MonitoringEvent> sensorStream26 = createSourceStream(13, rate, "OxigenLevel");
        // DataStream<MonitoringEvent> sensorStream27 = createSourceStream(14, rate, "HeartRate");
        // DataStream<MonitoringEvent> sensorStream28 = createSourceStream(14, rate, "OxigenLevel");
        // DataStream<MonitoringEvent> sensorStream29 = createSourceStream(15, rate, "HeartRate");
        // DataStream<MonitoringEvent> sensorStream30 = createSourceStream(15, rate, "OxigenLevel");
        // DataStream<MonitoringEvent> sensorStream31 = createSourceStream(16, rate, "HeartRate");
        // DataStream<MonitoringEvent> sensorStream32 = createSourceStream(16, rate, "OxigenLevel");

        DataStream<MonitoringEvent> filteredStream1 = sensorStream1.filter(new ThresholdFilter(20)).setParallelism(1);
        DataStream<MonitoringEvent> filteredStream2 = sensorStream2.filter(new ThresholdFilter(20)).setParallelism(1);
        // DataStream<MonitoringEvent> filteredStream3 = sensorStream3.filter(new ThresholdFilter(20)).setParallelism(1);
        // DataStream<MonitoringEvent> filteredStream4 = sensorStream4.filter(new ThresholdFilter(20)).setParallelism(1);
        // DataStream<MonitoringEvent> filteredStream5 = sensorStream5.filter(new ThresholdFilter(20)).setParallelism(1);
        // DataStream<MonitoringEvent> filteredStream6 = sensorStream6.filter(new ThresholdFilter(20)).setParallelism(1);
        // DataStream<MonitoringEvent> filteredStream7 = sensorStream7.filter(new ThresholdFilter(20)).setParallelism(1);
        // DataStream<MonitoringEvent> filteredStream8 = sensorStream8.filter(new ThresholdFilter(20)).setParallelism(1);
        // DataStream<MonitoringEvent> filteredStream9 = sensorStream9.filter(new ThresholdFilter(20)).setParallelism(1);
        // DataStream<MonitoringEvent> filteredStream10 = sensorStream10.filter(new ThresholdFilter(20)).setParallelism(1);
        // DataStream<MonitoringEvent> filteredStream11 = sensorStream11.filter(new ThresholdFilter(20)).setParallelism(1);
        // DataStream<MonitoringEvent> filteredStream12 = sensorStream12.filter(new ThresholdFilter(20)).setParallelism(1);
        // DataStream<MonitoringEvent> filteredStream13 = sensorStream13.filter(new ThresholdFilter(20)).setParallelism(1);
        // DataStream<MonitoringEvent> filteredStream14 = sensorStream14.filter(new ThresholdFilter(20)).setParallelism(1);
        // DataStream<MonitoringEvent> filteredStream15 = sensorStream15.filter(new ThresholdFilter(20)).setParallelism(1);
        // DataStream<MonitoringEvent> filteredStream16 = sensorStream16.filter(new ThresholdFilter(20)).setParallelism(1);
        // DataStream<MonitoringEvent> filteredStream17 = sensorStream17.filter(new ThresholdFilter(20)).setParallelism(1);
        // DataStream<MonitoringEvent> filteredStream18 = sensorStream18.filter(new ThresholdFilter(20)).setParallelism(1);
        // DataStream<MonitoringEvent> filteredStream19 = sensorStream19.filter(new ThresholdFilter(20)).setParallelism(1);
        // DataStream<MonitoringEvent> filteredStream20 = sensorStream20.filter(new ThresholdFilter(20)).setParallelism(1);
        // DataStream<MonitoringEvent> filteredStream21 = sensorStream21.filter(new ThresholdFilter(20)).setParallelism(1);
        // DataStream<MonitoringEvent> filteredStream22 = sensorStream22.filter(new ThresholdFilter(20)).setParallelism(1);
        // DataStream<MonitoringEvent> filteredStream23 = sensorStream23.filter(new ThresholdFilter(20)).setParallelism(1);
        // DataStream<MonitoringEvent> filteredStream24 = sensorStream24.filter(new ThresholdFilter(20)).setParallelism(1);
        // DataStream<MonitoringEvent> filteredStream25 = sensorStream25.filter(new ThresholdFilter(20)).setParallelism(1);
        // DataStream<MonitoringEvent> filteredStream26 = sensorStream26.filter(new ThresholdFilter(20)).setParallelism(1);
        // DataStream<MonitoringEvent> filteredStream27 = sensorStream27.filter(new ThresholdFilter(20)).setParallelism(1);
        // DataStream<MonitoringEvent> filteredStream28 = sensorStream28.filter(new ThresholdFilter(20)).setParallelism(1);
        // DataStream<MonitoringEvent> filteredStream29 = sensorStream29.filter(new ThresholdFilter(20)).setParallelism(1);
        // DataStream<MonitoringEvent> filteredStream30 = sensorStream30.filter(new ThresholdFilter(20)).setParallelism(1);
        // DataStream<MonitoringEvent> filteredStream31 = sensorStream31.filter(new ThresholdFilter(20)).setParallelism(1);
        // DataStream<MonitoringEvent> filteredStream32 = sensorStream32.filter(new ThresholdFilter(20)).setParallelism(1);

        filteredStream1.union(filteredStream1, filteredStream2)
                              // filteredStream3, filteredStream4,
                              // filteredStream5, filteredStream6)
                              // filteredStream7, filteredStream8,
                              // filteredStream9, filteredStream10)
                              // filteredStream11, filteredStream12,
                              // filteredStream13, filteredStream14, filteredStream15, filteredStream16,
                              // filteredStream17, filteredStream18, filteredStream19, filteredStream20,
                              // filteredStream21, filteredStream22, filteredStream23, filteredStream24,
                              // filteredStream25, filteredStream26, filteredStream27, filteredStream28,
                              // filteredStream29, filteredStream30, filteredStream31, filteredStream32)
                              .keyBy("patientId")
                              .window(TumblingProcessingTimeWindows.of(Time.milliseconds(1000/rate)))
                              .trigger(CountTrigger.of(2))
                              .reduce(new MeasurementReduceFunction()).setParallelism(1)
                              .map(new PowerMapper()).setParallelism(1)
                              .map(new LatencyMapper()).setParallelism(1)
                              .map(new PatientMapper()).setParallelism(1)
                              .writeAsText(outPath).setParallelism(1);


        env.execute("HospitalBenchmark");
    }

    public static DataStream<MonitoringEvent> createSourceStream(int id, int rate, String type){
        return env.addSource(new MonitoringEventSource(id, rate, type)).setParallelism(1).slotSharingGroup(Integer.toString(id) + type);
    }

    public static class MeasurementReduceFunction implements ReduceFunction<MonitoringEvent>{

        public MonitoringEvent reduce(MonitoringEvent v1, MonitoringEvent v2){
            MonitoringEvent out = new MonitoringEvent(v1.getID(), "Combined");
            out.setValue(v1.getValue() + v2.getValue());
            out.setCreationTime(v2.getCreationTime());
            return out;
        }
    }

    public static class ThresholdFilter implements FilterFunction<MonitoringEvent>{

        int maximum = 0;

        public ThresholdFilter(int maximum){
            this.maximum = maximum;
        }

        @Override
        public boolean filter(MonitoringEvent monitoringEvent) throws Exception {
            if (monitoringEvent.getValue() < maximum){
                return false;
            }

            return true;
        }
    }

    public static class PatientMapper implements MapFunction<MonitoringEvent, Long> {

          @Override
          public Long map(MonitoringEvent value) throws Exception {
              long result = value.getCreationTime();
              return result;
          }
    }

    public static class PowerMapper implements MapFunction<MonitoringEvent, MonitoringEvent> {

          @Override
          public MonitoringEvent map(MonitoringEvent value) throws Exception {
              int pow = (int)Math.pow(value.getValue(),3);
              value.setValue(pow);
              return value;
          }
    }

    public static class LatencyMapper extends RichMapFunction<MonitoringEvent, MonitoringEvent> {
        private transient long latency = 0;
        private transient long num = 0;

        @Override
        public void open(Configuration config) {
            getRuntimeContext()
                    .getMetricGroup()
                    .gauge("HospitalLatency", new Gauge<Long>() {
                        @Override
                        public Long getValue() {
                            if (num > 0) {
                                return latency / num;
                            } else {
                                return 0L;
                            }
                        }
                    });
        }

        @Override
        public MonitoringEvent map(MonitoringEvent value) throws Exception {
            long creationTime = value.getCreationTime();
            long arrivalTime = System.currentTimeMillis();
            long result = arrivalTime - creationTime;
            this.latency += result;
            this.num++;
            value.setCreationTime(result);
            return value;
        }
    }

}
