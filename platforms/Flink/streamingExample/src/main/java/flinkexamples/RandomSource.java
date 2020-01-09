package flinkexamples;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class RandomSource implements SourceFunction<Integer> {

    private Integer count = 0;
    private volatile boolean isRunning = true;
    private Random rand = new Random();

    @Override
    public void run(SourceContext<Integer> ctx) throws InterruptedException {
        while (isRunning) {
            int n = rand.nextInt(100);
            ctx.collect(n);
            Thread.sleep(3000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}