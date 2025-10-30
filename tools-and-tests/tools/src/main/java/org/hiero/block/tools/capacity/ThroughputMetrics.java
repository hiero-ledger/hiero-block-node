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
    private final AtomicReference<Instant> firstByteInWindow = new AtomicReference<>(null);
    private final AtomicReference<Instant> lastByteInWindow = new AtomicReference<>(null);

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
        if (firstByteInWindow.compareAndSet(null, now)) {
            lastReportTime = now;
        }
        lastByteInWindow.set(now);
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
        if (lastReportTime == null) {
            return;
        }

        final Instant now = Instant.now();
        final Duration since = Duration.between(lastReportTime, now);
        if (since.toMillis() < reportIntervalSeconds * 1_000L) {
            return;
        }

        final long bytesNow = totalBytes.get();
        final int blocksNow = totalBlocks.get();
        final int ackedNow = totalBlocksAcked.get();

        final long bytesDelta = bytesNow - bytesAtLastReport.getAndSet(bytesNow);
        final int blocksDelta = blocksNow - blocksAtLastReport.getAndSet(blocksNow);
        final int ackedDelta = ackedNow - ackedAtLastReport.getAndSet(ackedNow);

        final long millis = Math.max(1L, since.toMillis());
        final long kbDelta = bytesDelta / 1_024L;
        final long kbps = (kbDelta * 1_000L) / millis;

        if ("CLIENT".equals(role)) {
            System.out.printf(
                    "[%s] ΔKB: %,6d | KB/s: %,6d | ΔBlocks: %,3d | ΔACKs: %,3d | Totals ACKed/Blocks: %,d/%,d%n",
                    role, kbDelta, kbps, blocksDelta, ackedDelta, totalBlocksAcked.get(), totalBlocks.get());
        } else {
            System.out.printf(
                    "[%s] ΔKB: %,6d | KB/s: %,6d | ΔBlocks: %,3d | Total blocks: %,d%n",
                    role, kbDelta, kbps, blocksDelta, totalBlocks.get());
        }

        lastReportTime = now;
    }

    public void reportFinal() {
        final Instant start = firstByteInWindow.get();
        final Instant end = lastByteInWindow.get();
        if (start == null || end == null) {
            System.out.printf("%s stopped (no data)%n", role);
            return;
        }

        final long bytes = totalBytes.get();
        final long totalMillis = Math.max(0L, Duration.between(start, end).toMillis());
        final long totalKb = bytes / 1_024L;
        final long kbps = totalMillis == 0L ? 0L : (totalKb * 1_000L) / totalMillis;

        System.out.printf("=== FINAL %s THROUGHPUT REPORT ===%n", role);
        System.out.printf("Total bytes: %,9d (%,6d KB)%n", bytes, totalKb);
        System.out.printf("Total time (first->last byte): %,6d milliseconds%n", totalMillis);
        System.out.printf("Average throughput: %,6d KB/s%n", kbps);

        if ("CLIENT".equals(role)) {
            System.out.printf("Total blocks sent: %,d%n", totalBlocks.get());
            System.out.printf("Total blocks ACKed: %,d%n", totalBlocksAcked.get());
            if (totalBlocks.get() > 0) {
                final long avgPerBlock = totalMillis / totalBlocks.get();
                System.out.printf(
                        "Average time per block: %3d.%03d seconds%n", avgPerBlock / 1_000L, avgPerBlock % 1_000L);
            }
        } else {
            System.out.printf("Total blocks received: %,d%n", totalBlocks.get());
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
