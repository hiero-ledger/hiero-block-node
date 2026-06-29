// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification;

import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification.FailureType;
import org.hiero.block.node.spi.threading.ThreadPoolManager;

/**
 * Writes failing block bytes and metadata to disk for post-incident diagnostics.
 *
 * <p>Activated only when {@link VerificationConfig#dumpEnabled()} is {@code true}. Each unique
 * {@code (blockNumber, failureType)} combination is dumped at most once per process lifetime
 * (rate limiting), so a retried bad block does not flood disk.
 *
 * <p>Two files are written per dump:
 * <ul>
 *   <li>{@code block-<N>-<TYPE>.blk} — raw protobuf-encoded {@link BlockUnparsed} bytes
 *   <li>{@code block-<N>-<TYPE>.meta.json} — JSON sidecar with correlation id, failure
 *       category, BN identity, and HAPI version
 * </ul>
 *
 * <p>A scheduled daily task purges files older than {@link VerificationConfig#dumpRetentionDays()}.
 */
public class BadBlockDumper {
    private static final System.Logger LOGGER = System.getLogger(BadBlockDumper.class.getName());

    private final VerificationConfig config;
    private final String bnIdentity;

    /** Tracks which (blockNumber, failureType) pairs have already been dumped this session. */
    private final Set<String> dumpedKeys = ConcurrentHashMap.newKeySet();

    private ScheduledExecutorService scheduler;

    /**
     * Creates a {@code BadBlockDumper}.
     *
     * @param config the verification configuration
     * @param bnIdentity a human-readable identifier for this Block Node instance (e.g. hostname)
     */
    public BadBlockDumper(@NonNull final VerificationConfig config, @NonNull final String bnIdentity) {
        this.config = config;
        this.bnIdentity = bnIdentity;
    }

    /**
     * Starts the dumper: creates the dump directory and schedules the daily purge task.
     *
     * @param threadPoolManager source of scheduled executor services
     */
    public void start(@NonNull final ThreadPoolManager threadPoolManager) {
        if (config.dumpEnabled()) {
            try {
                Files.createDirectories(config.dumpDirectoryPath());
            } catch (final IOException e) {
                LOGGER.log(
                        System.Logger.Level.WARNING,
                        "Failed to create dump directory {0} — dump feature may not work",
                        config.dumpDirectoryPath(),
                        e);
            }
            Thread.UncaughtExceptionHandler handler = (thread, e) ->
                    LOGGER.log(System.Logger.Level.INFO, "Uncaught exception in thread: " + thread.getName(), e);
            scheduler = threadPoolManager.createSingleThreadScheduledExecutor("BadBlockDumpPurge", handler);
            scheduler.scheduleAtFixedRate(this::purgeOldDumps, 0, 1, TimeUnit.DAYS);
        }
    }

    /**
     * Stops the purge scheduler. Safe to call even if {@link #start} was never called.
     */
    public void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    /**
     * Dumps the failing block to disk if:
     * <ul>
     *   <li>dumping is enabled,
     *   <li>the notification carries block bytes, and
     *   <li>this {@code (blockNumber, failureType)} has not been dumped before.
     * </ul>
     *
     * @param notification the failed verification notification (must have {@code success=false})
     * @param hapiVersion the HAPI proto version from the block header, may be {@code null}
     * @param blockItems the raw block items captured at the failure site, may be {@code null}
     */
    public void attemptDump(
            @NonNull final VerificationNotification notification,
            final SemanticVersion hapiVersion,
            final List<BlockItemUnparsed> blockItems) {
        if (config.dumpEnabled()) {
            if (blockItems != null) {
                final long blockNumber = notification.blockNumber();
                final FailureType failureType = notification.failureInfo().failureType();
                final String baseName = "block-%d-%s".formatted(blockNumber, failureType);
                if (dumpedKeys.add(baseName)) {
                    final String correlationId = UUID.randomUUID().toString();
                    final Path blkFile = config.dumpDirectoryPath().resolve(baseName + ".blk");
                    final Path metaFile = config.dumpDirectoryPath().resolve(baseName + ".meta.json");
                    try {
                        writeBlockFile(blkFile, blockItems);
                        writeMetaFile(metaFile, correlationId, blockNumber, failureType, hapiVersion);
                        LOGGER.log(
                                System.Logger.Level.TRACE,
                                "Dumped failed block {0} (failureType={1}) to {2}",
                                blockNumber,
                                failureType,
                                blkFile);
                    } catch (final IOException e) {
                        LOGGER.log(
                                System.Logger.Level.WARNING,
                                "Failed to dump block {0} (failureType={1})",
                                blockNumber,
                                failureType,
                                e);
                        // clean up partial files
                        silentDelete(blkFile);
                        silentDelete(metaFile);
                    }
                }
            }
        }
    }

    private void writeBlockFile(@NonNull final Path path, @NonNull final List<BlockItemUnparsed> blockItems)
            throws IOException {
        final BlockUnparsed block =
                BlockUnparsed.newBuilder().blockItems(blockItems).build();
        try (final WritableStreamingData out =
                new WritableStreamingData(new BufferedOutputStream(Files.newOutputStream(path)))) {
            BlockUnparsed.PROTOBUF.write(block, out);
            out.flush();
        }
    }

    private void writeMetaFile(
            @NonNull final Path path,
            @NonNull final String correlationId,
            final long blockNumber,
            @NonNull final FailureType failureType,
            final SemanticVersion hapiVersion)
            throws IOException {
        final String hapiVersionStr = hapiVersion == null
                ? "unknown"
                : "%d.%d.%d".formatted(hapiVersion.major(), hapiVersion.minor(), hapiVersion.patch());
        final String json = """
                {
                  "correlationId": "%s",
                  "handlerId": "BlockVerificationServicePlugin",
                  "failureCategory": "%s",
                  "bnIdentity": "%s",
                  "hapiVersion": "%s",
                  "blockNumber": %d
                }
                """.formatted(correlationId, failureType, bnIdentity, hapiVersionStr, blockNumber);
        Files.writeString(path, json, StandardCharsets.UTF_8);
    }

    /** Deletes files in the dump directory whose last-modified time is older than the configured retention. */
    private void purgeOldDumps() {
        final Path dir = config.dumpDirectoryPath();
        if (Files.isDirectory(dir)) {
            final Instant cutoff = Instant.now().minus(config.dumpRetentionDays(), ChronoUnit.DAYS);
            try (final Stream<Path> stream = Files.list(dir)) {
                stream.filter(Files::isRegularFile).forEach(file -> {
                    try {
                        final BasicFileAttributes attrs = Files.readAttributes(file, BasicFileAttributes.class);
                        if (attrs.lastModifiedTime().toInstant().isBefore(cutoff)) {
                            Files.deleteIfExists(file);
                            LOGGER.log(System.Logger.Level.DEBUG, "Purged old dump file {0}", file);
                            removeDumpedKey(file);
                        }
                    } catch (final IOException e) {
                        LOGGER.log(System.Logger.Level.DEBUG, "Failed to check/delete dump file {0}", file, e);
                    }
                });
            } catch (final IOException e) {
                LOGGER.log(System.Logger.Level.DEBUG, "Failed to list dump directory {0} for purge", dir, e);
            }
        }
    }

    private void removeDumpedKey(@NonNull final Path file) {
        final String name = file.getFileName().toString();
        final int dotIdx = name.indexOf('.');
        if (dotIdx > 0) {
            dumpedKeys.remove(name.substring(0, dotIdx));
        }
    }

    private static void silentDelete(@NonNull final Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (final IOException ignored) {
            // best-effort cleanup
        }
    }
}
