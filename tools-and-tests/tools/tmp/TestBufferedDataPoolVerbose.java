import org.hiero.block.tools.utils.BufferedDataPool;
import com.hedera.pbj.runtime.io.buffer.BufferedData;

public class TestBufferedDataPoolVerbose {
    public static void main(String[] args) throws Exception {
        System.out.println("Starting verbose test");
        final int ITER = 10000;

        Thread consumer = new Thread(() -> {
            try {
                for (int i = 0; i < ITER; i++) {
                    int size = 128 + (i % 256);
                    BufferedData b = BufferedDataPool.getBuffer(size);
                    byte[] data = new byte[size];
                    b.writeBytes(data);
                    BufferedDataPool.returnBuffer(b);
                }
            } catch (Throwable t) {
                t.printStackTrace();
                System.exit(2);
            }
        }, "consumer");

        long start = System.nanoTime();
        consumer.start();
        consumer.join();
        long durMs = (System.nanoTime() - start) / 1_000_000L;
        System.out.println("Completed " + ITER + " iterations in " + durMs + " ms");
    }
}

// This temporary standalone verbose test was replaced by a JUnit test in:
// tools-and-tests/tools/src/test/java/org/hiero/block/tools/utils/BufferedDataPoolTest.java
// Kept as a placeholder to avoid accidental execution.
