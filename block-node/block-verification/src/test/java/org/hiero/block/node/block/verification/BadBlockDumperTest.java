// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.hedera.hapi.node.base.SemanticVersion;
import java.io.IOException;
import java.nio.file.FileSystem;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BadBlockDumperTest {

    private FileSystem jimfs;
    private Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        jimfs = Jimfs.newFileSystem(Configuration.unix());
        tempDir = jimfs.getPath("/bad-blocks");
        Files.createDirectories(tempDir);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (jimfs != null) {
            jimfs.close();
        }
    }

    private static VerificationNotification failureNotification(final long blockNumber) {
        return new VerificationNotification(
                false, FailureInfo.standard(FailureType.UNKNOWN_ERROR), blockNumber, null, null, BlockSource.PUBLISHER);
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
        final VerificationConfig config = generateVerificationConfig(false, 7);
        final BadBlockDumper dumper = new BadBlockDumper(config, "test-host");

        dumper.attemptDump(failureNotification(1L), null, BlockUnparsed.DEFAULT.blockItems());

        assertEquals(0L, countBlkFiles());
    }

    @Test
    void sameBlockAndFailureTypeNotDumpedTwice() throws IOException {
        final VerificationConfig config = generateVerificationConfig(true, 7);
        final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
        final BadBlockDumper dumper = new BadBlockDumper(config, "test-host");
        dumper.start(new TestThreadPoolManager<>(Executors.newVirtualThreadPerTaskExecutor(), scheduler));

        dumper.attemptDump(failureNotification(42L), null, BlockUnparsed.DEFAULT.blockItems());
        dumper.attemptDump(failureNotification(42L), null, BlockUnparsed.DEFAULT.blockItems());

        dumper.stop();
        assertEquals(1L, countBlkFiles());
    }

    @Test
    void stopWithoutStartIsNoOp() {
        final VerificationConfig config = generateVerificationConfig(false, 7);
        final BadBlockDumper dumper = new BadBlockDumper(config, "test-host");
        dumper.stop();
    }

    @Test
    void attemptDumpIsNoOpForNullBlock() throws IOException {
        final VerificationConfig config = generateVerificationConfig(true, 7);
        final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
        final BadBlockDumper dumper = new BadBlockDumper(config, "test-host");
        dumper.start(new TestThreadPoolManager<>(Executors.newVirtualThreadPerTaskExecutor(), scheduler));

        final VerificationNotification nullBlockNotification = new VerificationNotification(
                false, FailureInfo.standard(FailureType.UNKNOWN_ERROR), 1L, null, null, BlockSource.PUBLISHER);
        dumper.attemptDump(nullBlockNotification, null, null);

        dumper.stop();
        assertEquals(0L, countBlkFiles());
    }

    @Test
    void attemptDumpIncludesHapiVersionInMeta() throws IOException {
        final VerificationConfig config = generateVerificationConfig(true, 7);
        final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
        final BadBlockDumper dumper = new BadBlockDumper(config, "test-host");
        dumper.start(new TestThreadPoolManager<>(Executors.newVirtualThreadPerTaskExecutor(), scheduler));

        dumper.attemptDump(
                failureNotification(5L), new SemanticVersion(0, 72, 0, "", ""), BlockUnparsed.DEFAULT.blockItems());

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
        final VerificationConfig config = generateVerificationConfig(true, 1);
        final CapturingScheduledExecutor scheduler = new CapturingScheduledExecutor();
        final BadBlockDumper dumper = new BadBlockDumper(config, "test-host");
        dumper.start(new TestThreadPoolManager<>(Executors.newVirtualThreadPerTaskExecutor(), scheduler));

        // Dump block 7 then backdate its files beyond the 1-day retention window
        dumper.attemptDump(failureNotification(7L), null, BlockUnparsed.DEFAULT.blockItems());
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
        dumper.attemptDump(failureNotification(8L), null, BlockUnparsed.DEFAULT.blockItems());
        assertEquals(2L, countBlkFiles());

        scheduler.runPurge();

        dumper.stop();
        assertEquals(1L, countBlkFiles());
    }

    @Test
    void purgeAllowsReDumpOfSameBlock() throws IOException {
        final VerificationConfig config = generateVerificationConfig(true, 1);
        final CapturingScheduledExecutor scheduler = new CapturingScheduledExecutor();
        final BadBlockDumper dumper = new BadBlockDumper(config, "test-host");
        dumper.start(new TestThreadPoolManager<>(Executors.newVirtualThreadPerTaskExecutor(), scheduler));

        dumper.attemptDump(failureNotification(7L), null, BlockUnparsed.DEFAULT.blockItems());
        assertEquals(1L, countBlkFiles());

        // Backdate block 7 files and purge - this must remove the dedup key
        final Instant twoDaysAgo = Instant.now().minus(2, ChronoUnit.DAYS);
        try (final Stream<Path> stream = Files.list(tempDir)) {
            stream.forEach(f -> {
                try {
                    Files.setLastModifiedTime(f, FileTime.from(twoDaysAgo));
                } catch (final IOException ignored) {
                }
            });
        }
        scheduler.runPurge();
        assertEquals(0L, countBlkFiles());

        // After purge the same block must be dumpable again
        dumper.attemptDump(failureNotification(7L), null, BlockUnparsed.DEFAULT.blockItems());

        dumper.stop();
        assertEquals(1L, countBlkFiles());
    }

    @Test
    void startHandlesDirectoryCreationFailureGracefully() throws IOException {
        // Replace the dump directory with a regular file so Files.createDirectories throws
        Files.delete(tempDir);
        Files.createFile(tempDir);
        final VerificationConfig config = generateVerificationConfig(true, 7);
        final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
        final BadBlockDumper dumper = new BadBlockDumper(config, "test-host");
        dumper.start(new TestThreadPoolManager<>(Executors.newVirtualThreadPerTaskExecutor(), scheduler));
        dumper.stop();
    }

    @Test
    void attemptDumpHandlesWriteFailureGracefully() throws IOException {
        final VerificationConfig config = generateVerificationConfig(true, 7);
        final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
        final BadBlockDumper dumper = new BadBlockDumper(config, "test-host");
        dumper.start(new TestThreadPoolManager<>(Executors.newVirtualThreadPerTaskExecutor(), scheduler));
        // Delete the dump directory so file creation fails inside attemptDump
        Files.delete(tempDir);
        dumper.attemptDump(failureNotification(99L), null, BlockUnparsed.DEFAULT.blockItems());
        dumper.stop();
    }

    private VerificationConfig generateVerificationConfig(final boolean dumpEnabled, final int dumpRetentionDays) {
        return new VerificationConfig(100, 100, 0L, true, dumpEnabled, tempDir, dumpRetentionDays);
    }
}
