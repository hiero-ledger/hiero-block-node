// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.node.base.SemanticVersion;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.async.TestThreadPoolManager;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification.FailureInfo;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification.FailureType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BadBlockDumperTest {

    @TempDir
    Path tempDir;

    private static VerificationNotification failureNotification(final long blockNumber) {
        return new VerificationNotification(
                false,
                FailureInfo.standard(FailureType.UNKNOWN_ERROR),
                blockNumber,
                null,
                BlockUnparsed.DEFAULT,
                BlockSource.PUBLISHER);
    }

    private long countBlkFiles() throws IOException {
        try (final Stream<Path> stream = Files.list(tempDir)) {
            return stream.filter(f -> f.getFileName().toString().endsWith(".blk"))
                    .count();
        }
    }

    /**
     * Captures the Runnable registered via scheduleAtFixedRate so tests can invoke
     * purgeOldDumps() directly without relying on background scheduling.
     */
    private static final class CapturingScheduledExecutor extends ScheduledThreadPoolExecutor {
        private Runnable capturedTask;

        CapturingScheduledExecutor() {
            super(1);
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(
                final Runnable command, final long initialDelay, final long period, final TimeUnit unit) {
            this.capturedTask = command;
            return super.scheduleAtFixedRate(command, Long.MAX_VALUE, period, unit);
        }

        void runPurge() {
            capturedTask.run();
        }
    }

    @Test
    void attemptDumpIsNoOpWhenDisabled() throws IOException {
        final VerificationConfig config =
                new VerificationConfig(Path.of(""), false, false, 100, Path.of(""), 100, false, tempDir, 7);
        final BadBlockDumper dumper = new BadBlockDumper(config, "test-host");

        dumper.attemptDump(failureNotification(1L), null);

        assertEquals(0L, countBlkFiles());
    }

    @Test
    void sameBlockAndFailureTypeNotDumpedTwice() throws IOException {
        final VerificationConfig config =
                new VerificationConfig(Path.of(""), false, false, 100, Path.of(""), 100, true, tempDir, 7);
        final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
        final BadBlockDumper dumper = new BadBlockDumper(config, "test-host");
        dumper.start(new TestThreadPoolManager<>(Executors.newVirtualThreadPerTaskExecutor(), scheduler));

        dumper.attemptDump(failureNotification(42L), null);
        dumper.attemptDump(failureNotification(42L), null);

        dumper.stop();
        assertEquals(1L, countBlkFiles());
    }

    @Test
    void stopWithoutStartIsNoOp() {
        final VerificationConfig config =
                new VerificationConfig(Path.of(""), false, false, 100, Path.of(""), 100, false, tempDir, 7);
        final BadBlockDumper dumper = new BadBlockDumper(config, "test-host");
        dumper.stop();
    }

    @Test
    void attemptDumpIsNoOpForNullBlock() throws IOException {
        final VerificationConfig config =
                new VerificationConfig(Path.of(""), false, false, 100, Path.of(""), 100, true, tempDir, 7);
        final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
        final BadBlockDumper dumper = new BadBlockDumper(config, "test-host");
        dumper.start(new TestThreadPoolManager<>(Executors.newVirtualThreadPerTaskExecutor(), scheduler));

        final VerificationNotification nullBlockNotification = new VerificationNotification(
                false, FailureInfo.standard(FailureType.UNKNOWN_ERROR), 1L, null, null, BlockSource.PUBLISHER);
        dumper.attemptDump(nullBlockNotification, null);

        dumper.stop();
        assertEquals(0L, countBlkFiles());
    }

    @Test
    void attemptDumpIncludesHapiVersionInMeta() throws IOException {
        final VerificationConfig config =
                new VerificationConfig(Path.of(""), false, false, 100, Path.of(""), 100, true, tempDir, 7);
        final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
        final BadBlockDumper dumper = new BadBlockDumper(config, "test-host");
        dumper.start(new TestThreadPoolManager<>(Executors.newVirtualThreadPerTaskExecutor(), scheduler));

        dumper.attemptDump(failureNotification(5L), new SemanticVersion(0, 72, 0, "", ""));

        dumper.stop();
        assertEquals(1L, countBlkFiles());
        try (final Stream<Path> stream = Files.list(tempDir)) {
            final Path metaFile = stream.filter(f -> f.getFileName().toString().endsWith(".meta.json"))
                    .findFirst()
                    .orElseThrow();
            assertTrue(Files.readString(metaFile).contains("0.72.0"), "Meta file must contain the HAPI version");
        }
    }

    @Test
    void purgeRemovesFilesOlderThanRetention() throws IOException {
        final VerificationConfig config =
                new VerificationConfig(Path.of(""), false, false, 100, Path.of(""), 100, true, tempDir, 1);
        final CapturingScheduledExecutor scheduler = new CapturingScheduledExecutor();
        final BadBlockDumper dumper = new BadBlockDumper(config, "test-host");
        dumper.start(new TestThreadPoolManager<>(Executors.newVirtualThreadPerTaskExecutor(), scheduler));

        // Dump block 7 then backdate its files beyond the 1-day retention window
        dumper.attemptDump(failureNotification(7L), null);
        final Instant twoDaysAgo = Instant.now().minus(2, ChronoUnit.DAYS);
        try (final Stream<Path> stream = Files.list(tempDir)) {
            stream.forEach(f -> {
                try {
                    Files.setLastModifiedTime(f, FileTime.from(twoDaysAgo));
                } catch (final IOException ignored) {
                }
            });
        }

        // Dump block 8 with a current timestamp; this file must survive the purge
        dumper.attemptDump(failureNotification(8L), null);
        assertEquals(2L, countBlkFiles());

        scheduler.runPurge();

        dumper.stop();
        assertEquals(1L, countBlkFiles());
    }
}
