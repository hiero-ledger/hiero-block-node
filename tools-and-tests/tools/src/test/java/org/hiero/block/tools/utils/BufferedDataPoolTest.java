// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.utils;

import static org.junit.jupiter.api.Assertions.*;

import com.hedera.pbj.runtime.io.buffer.BufferedData;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class BufferedDataPoolTest {
    @Test
    void testAllocateAndCapacity() {
        final int size = 512;
        final BufferedData b = BufferedDataPool.getBuffer(size);
        assertTrue(b.capacity() >= size, "BufferedData capacity should be at least requested size");
        BufferedDataPool.returnBuffer(b);
    }

    @Test
    @Timeout(value = 10)
    void testSingleProducerSingleConsumer() throws Exception {
        final int ITER = 10_000;
        final BlockingQueue<BufferedData> q = new ArrayBlockingQueue<>(1024);

        try (ExecutorService es = Executors.newFixedThreadPool(2)) {
            Callable<Void> producer = () -> {
                for (int i = 0; i < ITER; i++) {
                    int size = 128 + (i % 256);
                    BufferedData b = BufferedDataPool.getBuffer(size);
                    // write some bytes to advance position
                    b.writeBytes(new byte[size]);
                    q.put(b);
                }
                return null;
            };

            Callable<Void> consumer = () -> {
                for (int i = 0; i < ITER; i++) {
                    BufferedData b = q.take();
                    // return to pool
                    BufferedDataPool.returnBuffer(b);
                }
                return null;
            };

            Future<Void> f1 = es.submit(producer);
            Future<Void> f2 = es.submit(consumer);

            // wait for completion
            f1.get();
            f2.get();
            es.shutdownNow();
        }
    }

    @Test
    void testReuseLikely() {
        // Populate pool with a number of buffers of varying sizes
        for (int i = 0; i < 50; i++) {
            BufferedDataPool.returnBuffer(BufferedData.wrap(new byte[128 + i]));
        }
        // Request a small buffer and assert capacity is sufficient
        BufferedData b = BufferedDataPool.getBuffer(64);
        assertTrue(b.capacity() >= 64);
        BufferedDataPool.returnBuffer(b);
    }
}
