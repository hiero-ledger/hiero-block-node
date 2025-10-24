// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.capacity;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Lazily starts the measurement on the first byte/block seen and
 * computes final duration using [firstByteAt, lastByteAt].
 * Periodic reports are based on deltas since the last report.
 */
public class ThroughputMetrics {
    private final AtomicLong totalBytes = new AtomicLong(0);
    private final AtomicInteger totalBlocks = new AtomicInteger(0);
    private final AtomicInteger totalBlocksAcked = new AtomicInteger(0);

    // Actual data window
    private final AtomicReference<Instant> firstByteAt = new AtomicReference<>(null);
    private final AtomicReference<Instant> lastByteAt = new AtomicReference<>(null);

    // Periodic delta tracking
    private final AtomicLong bytesAtLastReport = new AtomicLong(0);
    private final AtomicInteger blocksAtLastReport = new AtomicInteger(0);
    private final AtomicInteger ackedAtLastReport = new AtomicInteger(0);
    private volatile Instant lastReportTime;

    private final long reportIntervalSeconds;
    private final String role; // "CLIENT" or "SERVER"

    public ThroughputMetrics(String role, long reportIntervalSeconds) {
        this.role = role;
        this.reportIntervalSeconds = reportIntervalSeconds;
    }

    private void markFirstData() {
        final Instant now = Instant.now();
        if (firstByteAt.compareAndSet(null, now)) {
            lastReportTime = now;
        }
        lastByteAt.set(now);
    }

    public void addBytes(long bytes) {
        if (bytes <= 0) return;
        markFirstData();
        totalBytes.addAndGet(bytes);
    }

    public void incrementBlocks() {
        markFirstData();
        totalBlocks.incrementAndGet();
    }

    public void incrementAckedBlocks() {
        markFirstData();
        totalBlocksAcked.incrementAndGet();
    }

    public void reportPeriodic() {
        if (lastReportTime == null) return;

        final Instant now = Instant.now();
        final Duration since = Duration.between(lastReportTime, now);
        if (since.getSeconds() < reportIntervalSeconds) return;

        final long bytesNow = totalBytes.get();
        final int blocksNow = totalBlocks.get();
        final int ackedNow = totalBlocksAcked.get();

        final long bytesDelta = bytesNow - bytesAtLastReport.getAndSet(bytesNow);
        final int blocksDelta = blocksNow - blocksAtLastReport.getAndSet(blocksNow);
        final int ackedDelta = ackedNow - ackedAtLastReport.getAndSet(ackedNow);

        final double secs = Math.max(1e-9, since.toMillis() / 1000.0);
        final double mbDelta = bytesDelta / (1024.0 * 1024.0);
        final double mbps = mbDelta / secs;

        if ("CLIENT".equals(role)) {
            System.out.printf(
                    "[%s] %.2f MB/s | +%.2f MB in %.2fs %n",
                    role, mbps, mbDelta, secs, blocksDelta, ackedDelta, totalBlocksAcked.get(), totalBlocks.get());
        } else {
            System.out.printf(
                    "[%s] %.2f MB/s | +%.2f MB in %.2fs %n", role, mbps, mbDelta, secs, blocksDelta, totalBlocks.get());
        }

        lastReportTime = now;
    }

    public void reportFinal() {
        final Instant start = firstByteAt.get();
        final Instant end = lastByteAt.get();
        if (start == null || end == null) {
            System.out.printf("%s stopped (no data)%n", role);
            return;
        }

        final long bytes = totalBytes.get();
        double totalSeconds = Math.max(1e-9, Duration.between(start, end).toMillis() / 1000.0);
        final double totalMB = bytes / (1024.0 * 1024.0);
        final double mbps = totalMB / totalSeconds;

        System.out.printf("=== FINAL %s THROUGHPUT REPORT ===%n", role);
        System.out.printf("Total bytes: %d (%.2f MB)%n", bytes, totalMB);
        System.out.printf("Total time (first->last byte): %.2f seconds%n", totalSeconds);
        System.out.printf("Average throughput: %.2f MB/s%n", mbps);

        if ("CLIENT".equals(role)) {
            System.out.printf("Total blocks sent: %d%n", totalBlocks.get());
            System.out.printf("Total blocks ACKed: %d%n", totalBlocksAcked.get());
            if (totalBlocks.get() > 0) {
                final double avgPerBlock = totalSeconds / totalBlocks.get();
                System.out.printf("Average time per block: %.3f seconds%n", avgPerBlock);
            }
        } else {
            System.out.printf("Total blocks received: %d%n", totalBlocks.get());
        }
    }

    // Accessors
    public int getTotalBlocks() {
        return totalBlocks.get();
    }

    public int getTotalBlocksAcked() {
        return totalBlocksAcked.get();
    }
}
