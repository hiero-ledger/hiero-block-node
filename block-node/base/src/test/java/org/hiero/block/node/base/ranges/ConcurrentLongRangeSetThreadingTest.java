package org.hiero.block.node.base.ranges;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for thread safety of {@link ConcurrentLongRangeSet}.
 * These tests focus on concurrent operations to verify that the class is thread-safe and lock-free.
 */
class ConcurrentLongRangeSetThreadingTest {

    private static final int THREAD_COUNT = Runtime.getRuntime().availableProcessors();
    private static final int OPERATIONS_PER_THREAD = 1000;

    /**
     * Tests concurrent additions to the set from multiple threads.
     */
    @Test
    @DisplayName("Concurrent additions should not lose or corrupt data")
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testConcurrentAdditions() throws Exception {
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet();
        final CountDownLatch startLatch = new CountDownLatch(1);
        final List<Future<?>> futures = new ArrayList<>();

        try (final ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT)){
            // Each thread will add different ranges to avoid overlap
            for (int t = 0; t < THREAD_COUNT; t++) {
                final int threadIndex = t;
                futures.add(executor.submit(() -> {
                    try {
                        startLatch.await(); // Wait for all threads to be ready
                        final int base = threadIndex * OPERATIONS_PER_THREAD * 3;
                        for (int i = 0; i < OPERATIONS_PER_THREAD; i++) {
                            final long start = base + (i * 2);
                            final long end = start + 1;
                            set.add(start, end);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }));
            }

            startLatch.countDown(); // Start all threads

            // Wait for all threads to complete
            for (Future<?> future : futures) {
                future.get();
            }

            // Verify all ranges were added correctly
            assertEquals(THREAD_COUNT * OPERATIONS_PER_THREAD * 2L, set.size());
            assertEquals(THREAD_COUNT, set.rangeCount());

            // Verify each specific range was added
            for (int t = 0; t < THREAD_COUNT; t++) {
                final int base = t * OPERATIONS_PER_THREAD * 3;
                for (int i = 0; i < OPERATIONS_PER_THREAD; i++) {
                    final long start = base + (i * 2);
                    final long end = start + 1;
                    assertTrue(set.contains(start, end), "Missing range: " + start + "-" + end);
                }
            }
        }
    }

    /**
     * Tests concurrent removals from the set from multiple threads.
     */
    @Test
    @DisplayName("Concurrent removals should not corrupt data")
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testConcurrentRemovals() throws Exception {
        // Initialize with a large continuous range
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet(
                0, THREAD_COUNT * OPERATIONS_PER_THREAD * 2L - 1);
        
        final CountDownLatch startLatch = new CountDownLatch(1);
        final List<Future<?>> futures = new ArrayList<>();

        try (final ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT)){
            // Each thread will remove different ranges to avoid overlap
            for (int t = 0; t < THREAD_COUNT; t++) {
                final int threadIndex = t;
                futures.add(executor.submit(() -> {
                    try {
                        startLatch.await(); // Wait for all threads to be ready
                        final int base = threadIndex * OPERATIONS_PER_THREAD * 2;
                        for (int i = 0; i < OPERATIONS_PER_THREAD; i++) {
                            final long start = base + (i * 2);
                            final long end = start + 1;
                            set.remove(start, end);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }));
            }

            startLatch.countDown(); // Start all threads

            // Wait for all threads to complete
            for (Future<?> future : futures) {
                future.get();
            }

            // Verify all ranges were removed (set should be empty)
            assertEquals(0, set.size());
            assertEquals(0, set.rangeCount());
        }
    }

    /**
     * Tests concurrent mixed operations (both adds and removes) from multiple threads.
     */
    @Test
    @DisplayName("Concurrent mixed operations should not corrupt data")
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testConcurrentMixedOperations() throws Exception {
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet();
        final CountDownLatch startLatch = new CountDownLatch(1);
        final List<Future<?>> futures = new ArrayList<>();
        
        // Generate non-overlapping ranges for each thread
        final int rangeSize = OPERATIONS_PER_THREAD * 10;
        
        try (final ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT)){
            // Half of the threads will add, half will remove
            for (int t = 0; t < THREAD_COUNT; t++) {
                final int threadIndex = t;
                final boolean isAdder = threadIndex % 2 == 0;
                
                futures.add(executor.submit(() -> {
                    try {
                        startLatch.await(); // Wait for all threads to be ready
                        final int base = threadIndex * rangeSize;
                        final ThreadLocalRandom random = ThreadLocalRandom.current();
                        
                        for (int i = 0; i < OPERATIONS_PER_THREAD; i++) {
                            // Create a range of random length within thread's area
                            final long rangeStart = base + random.nextInt(rangeSize - 10);
                            final long rangeEnd = rangeStart + random.nextInt(10);
                            
                            if (isAdder) {
                                set.add(rangeStart, rangeEnd);
                            } else {
                                set.remove(rangeStart, rangeEnd);
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }));
            }

            startLatch.countDown(); // Start all threads

            // Wait for all threads to complete
            for (Future<?> future : futures) {
                future.get();
            }
            
            // We can't assert the exact size since operations are random and may overlap
            // But we can verify the set is in a consistent state by checking that all ranges are valid
            set.streamRanges().forEach(range -> assertTrue(range.start() <= range.end()));
        }
    }
    
    /**
     * Tests high contention scenario with many threads modifying the same range.
     */
    @Test
    @DisplayName("High contention operations should complete without deadlock or corruption")
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void testHighContention() throws Exception {
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet();
        final CountDownLatch startLatch = new CountDownLatch(1);
        final List<Future<?>> futures = new ArrayList<>();
        
        final int CONTENDED_RANGE = 100; // Small range that all threads will contend for
        
        try (final ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT * 2)){
            // All threads will modify the same range
            for (int t = 0; t < THREAD_COUNT * 2; t++) {
                futures.add(executor.submit(() -> {
                    try {
                        startLatch.await(); // Wait for all threads to be ready
                        final ThreadLocalRandom random = ThreadLocalRandom.current();
                        
                        for (int i = 0; i < OPERATIONS_PER_THREAD; i++) {
                            final long start = random.nextInt(CONTENDED_RANGE);
                            final long end = start + random.nextInt(CONTENDED_RANGE - (int)start);
                            
                            if (random.nextBoolean()) {
                                set.add(start, end);
                            } else {
                                set.remove(start, end);
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }));
            }

            startLatch.countDown(); // Start all threads

            // Wait for all threads to complete
            for (Future<?> future : futures) {
                future.get(); // This will throw if any thread encountered an exception
            }
            
            // Verify the set is in a consistent state
            set.streamRanges().forEach(range -> {
                assertTrue(range.start() <= range.end(), "Invalid range: " + range);
                assertTrue(range.start() >= 0, "Range start negative: " + range);
                assertTrue(range.end() < CONTENDED_RANGE, "Range end too large: " + range);
            });
        }
    }
    
    /**
     * Tests that operations are lock-free by running a long operation with a pause
     * and verifying that other threads aren't blocked.
     */
    @Test
    @DisplayName("Operations should be lock-free")
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testOperationsAreLockFree() throws Exception {
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet();
        final AtomicBoolean slowThreadRunning = new AtomicBoolean(true);
        final AtomicInteger completedOperations = new AtomicInteger(0);
        final CountDownLatch slowThreadStarted = new CountDownLatch(1);
        
        try (final ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT)){
            // A slow thread that will make one big change and sleep in the middle
            Future<?> slowThread = executor.submit(() -> {
                try {
                    // Add a very large range that will take time to merge
                    set.add(0, 1_000_000);
                    slowThreadStarted.countDown();
                    
                    // Sleep to simulate a long operation
                    Thread.sleep(500);
                    
                    // Remove a portion of the range
                    set.remove(250_000, 750_000);
                    
                    slowThreadRunning.set(false);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            
            // Wait for slow thread to start
            slowThreadStarted.await();
            
            // Start fast threads that should not be blocked
            List<Future<?>> fastThreads = new ArrayList<>();
            for (int t = 0; t < THREAD_COUNT - 1; t++) {
                fastThreads.add(executor.submit(() -> {
                    int base = 2_000_000 + ThreadLocalRandom.current().nextInt(1_000_000);
                    while (slowThreadRunning.get()) {
                        // Do quick operations on a different range
                        set.add(base, base + 10);
                        set.remove(base + 5, base + 7);
                        completedOperations.incrementAndGet();
                    }
                }));
            }
            
            // Wait for slow thread to complete
            slowThread.get();
            
            // Wait for fast threads to complete
            for (Future<?> future : fastThreads) {
                future.get(1, TimeUnit.SECONDS);
            }
            
            // If operations were not blocked by the slow thread, we should have completed many operations
            int completed = completedOperations.get();
            assertTrue(completed > 100, 
                "Expected at least 100 operations to complete during slow operation, but got: " + completed);
        }
    }
    
    /**
     * Tests performance under varying levels of thread contention.
     */
    @ParameterizedTest
    @ValueSource(ints = {1, 2, 4, 8, 16})
    @DisplayName("Performance should scale reasonably with thread count")
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testConcurrentPerformance(int threadCount) throws Exception {
        // Skip this test if we don't have enough cores for the higher thread counts
        if (threadCount > Runtime.getRuntime().availableProcessors() * 2) {
            return;
        }
        
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet();
        final CountDownLatch startLatch = new CountDownLatch(1);
        final List<Future<?>> futures = new ArrayList<>();
        
        final int operationsPerThread = 10_000; // More operations for performance test
        
        try (final ExecutorService executor = Executors.newFixedThreadPool(threadCount)){
            // Each thread performs its own operations in its own range
            for (int t = 0; t < threadCount; t++) {
                final int threadIndex = t;
                futures.add(executor.submit(() -> {
                    try {
                        startLatch.await();
                        final int base = threadIndex * operationsPerThread * 2;
                        final ThreadLocalRandom random = ThreadLocalRandom.current();
                        
                        for (int i = 0; i < operationsPerThread; i++) {
                            final long start = base + i * 2;
                            final long end = start + 1;
                            
                            // Mix of operations
                            if (i % 4 == 0) {
                                set.add(start, end);
                            } else if (i % 4 == 1) {
                                set.remove(start, end);
                            } else if (i % 4 == 2) {
                                set.contains(start, end);
                            } else {
                                set.contains(random.nextLong(base, base + operationsPerThread * 2));
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }));
            }

            // Measure how long the operations take
            long startTime = System.nanoTime();
            startLatch.countDown(); // Start all threads

            // Wait for all threads to complete
            for (Future<?> future : futures) {
                future.get();
            }
            
            long endTime = System.nanoTime();
            long durationMs = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
            
            // We're not making strict assertions about performance,
            // but we log it for manual inspection
            System.out.printf("Thread count: %d, Duration: %d ms, Operations/sec: %,.0f%n",
                    threadCount, durationMs, 
                    (double)(threadCount * operationsPerThread) / (durationMs / 1000.0));
        }
    }
}
