// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import com.hedera.hapi.block.stream.output.StateIdentifier;
import com.hedera.hapi.node.state.blockstream.BlockStreamInfo;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.state.merkle.VirtualMapState;
import com.swirlds.state.merkle.VirtualMapStateLifecycleManager;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import org.hiero.base.crypto.Hash;

/// On-disk handling of recent state snapshots under one snapshot root: restoring the newest
/// snapshot, writing a new one, and pruning old and staging directories.
///
/// The layout is one `<blockNumber>` directory per complete snapshot. A write goes to a
/// `.tmp-<blockNumber>` staging directory that is atomically renamed once complete, so a
/// numeric directory is always a whole snapshot. Anything else under the root (in particular
/// `tmp`, which holds the live MerkleDb files) is never touched.
///
/// This class owns the "how and where on disk". It holds no plugin state: no halt flags, no
/// attested state, no metrics and no executors. `StateManagementPlugin` decides when to restore
/// or write and what to write, reserves the state, and reacts to the returned outcomes (logging
/// restore results, halting, updating metrics).
///
/// Not thread-safe. `write`, `deleteStagingDirectories` and `restoreLatest` must be confined to
/// one thread at a time; the plugin runs writes only on its snapshot thread and restore and
/// staging cleanup only during start-up, before that thread exists.
final class StateSnapshotManager {

    private static final System.Logger LOGGER = System.getLogger(StateSnapshotManager.class.getName());

    /// Prefix of the staging directory a snapshot is written to before it is renamed to its
    /// final `<blockNumber>` name.
    private static final String STAGING_PREFIX = ".tmp-";

    private final Path root;
    private final int retentionCount;
    private final VirtualMapStateLifecycleManager lifecycleManager;

    /// Result of `restoreLatest`.
    sealed interface RestoreOutcome permits NoSnapshot, Restored, Failed {}

    /// No numbered snapshot directory exists (or the root does not exist): a genesis start.
    record NoSnapshot() implements RestoreOutcome {}

    /// The newest snapshot was loaded into the lifecycle manager and passed its self-check.
    ///
    /// @param blockNumber the restored block number (the directory name, equal to the state's own)
    /// @param rootHash the root hash `loadSnapshot` returned
    /// @param directory the directory the snapshot was loaded from
    record Restored(
            long blockNumber,
            @NonNull Hash rootHash,
            @NonNull Path directory) implements RestoreOutcome {}

    /// Snapshots may exist but could not be restored. The caller must not fall back to genesis.
    ///
    /// @param kind what went wrong
    /// @param directory the snapshot directory involved, or `null` for `UNLISTABLE_ROOT`
    /// @param expectedBlock the block number from the directory name, or `-1` for
    ///     `UNLISTABLE_ROOT`
    /// @param foundBlock the block number the loaded state reports, set only for
    ///     `SELF_CHECK_MISMATCH`, else `-1`
    /// @param cause the exception, or `null` for `SELF_CHECK_MISMATCH`
    record Failed(
            @NonNull FailureKind kind,
            @Nullable Path directory,
            long expectedBlock,
            long foundBlock,
            @Nullable Throwable cause)
            implements RestoreOutcome {}

    /// Why a restore failed.
    enum FailureKind {
        /// The snapshot root exists but cannot be listed.
        UNLISTABLE_ROOT,
        /// `loadSnapshot` (or reading the loaded state) threw.
        UNREADABLE,
        /// The loaded state's `BlockStreamInfo.block_number` disagrees with its directory name.
        SELF_CHECK_MISMATCH
    }

    /// Result status of `write`.
    enum WriteStatus {
        /// A new snapshot directory was written, and old snapshots were pruned.
        WRITTEN,
        /// A complete snapshot directory for the block already existed; nothing was written.
        ALREADY_PRESENT,
        /// The write failed; the staging directory was removed on a best-effort basis.
        FAILED
    }

    /// Result of `write`.
    ///
    /// @param status what happened
    /// @param durationMs time spent writing and renaming the snapshot, `0` unless `WRITTEN`
    record WriteResult(@NonNull WriteStatus status, long durationMs) {}

    /// @param root the snapshot root directory (`snapshotRecentPath`)
    /// @param retentionCount how many numbered snapshot directories to keep after a write
    /// @param lifecycleManager the lifecycle manager restored snapshots are loaded into
    StateSnapshotManager(
            @NonNull final Path root,
            final int retentionCount,
            @NonNull final VirtualMapStateLifecycleManager lifecycleManager) {
        this.root = Objects.requireNonNull(root);
        this.retentionCount = retentionCount;
        this.lifecycleManager = Objects.requireNonNull(lifecycleManager);
    }

    /// Load the highest-numbered snapshot directory into the lifecycle manager and check that
    /// the loaded state's own `BlockStreamInfo.block_number` matches the directory name (the
    /// self-check of `docs/design/state/live-state.md` §5.1/§6.2). Logs nothing: the caller
    /// logs and decides whether to halt.
    ///
    /// @return `NoSnapshot`, `Restored`, or `Failed` with the reason
    @NonNull
    RestoreOutcome restoreLatest() {
        final long candidate;
        try {
            candidate = highestNumberedSnapshotDirectory();
        } catch (final IOException e) {
            return new Failed(FailureKind.UNLISTABLE_ROOT, null, -1L, -1L, e);
        }
        if (candidate < 0L) {
            return new NoSnapshot();
        }
        final Path snapshotDir = recentSnapshotDirectoryFor(candidate);
        try {
            final Hash loadedHash = lifecycleManager.loadSnapshot(snapshotDir);
            final long loadedBlockNumber = blockNumberOf(lifecycleManager.getLatestImmutableState());
            if (loadedBlockNumber == candidate) {
                return new Restored(candidate, loadedHash, snapshotDir);
            }
            return new Failed(FailureKind.SELF_CHECK_MISMATCH, snapshotDir, candidate, loadedBlockNumber, null);
        } catch (final IOException | RuntimeException e) {
            // loadSnapshot wraps most on-disk problems (missing/corrupt MerkleDb files) in an
            // unchecked exception rather than a plain IOException, so catch both.
            return new Failed(FailureKind.UNREADABLE, snapshotDir, candidate, -1L, e);
        }
    }

    /// Write `state` to `<root>/<blockNumber>`, then prune old snapshots. The write goes to a
    /// `.tmp-<blockNumber>` staging dir that is atomically renamed once complete. An existing
    /// `<blockNumber>` dir counts as complete, since only complete snapshots are ever renamed
    /// into place.
    ///
    /// Calls `VirtualMap.createSnapshot` directly rather than
    /// `VirtualMapStateLifecycleManager.createSnapshot`, which catches and only logs write
    /// errors, so a failed write would look like success.
    ///
    /// The caller must hold a reservation on `state`'s root for the duration of the call. Not
    /// thread-safe: two concurrent calls could write and prune the same staging dir, so callers
    /// confine it to one thread. Prunes only after `WRITTEN`.
    ///
    /// @param state the state to write, reserved by the caller
    /// @param blockNumber the block number `state` corresponds to
    /// @return the outcome and, for `WRITTEN`, the write duration
    @NonNull
    WriteResult write(@NonNull final VirtualMapState state, final long blockNumber) {
        final Path target = recentSnapshotDirectoryFor(blockNumber);
        final Path staging = root.resolve(STAGING_PREFIX + blockNumber);
        final long durationMs;
        try {
            if (Files.isDirectory(target)) {
                return new WriteResult(WriteStatus.ALREADY_PRESENT, 0L);
            }
            Files.createDirectories(root);
            deleteRecursively(staging);
            final long startNanos = System.nanoTime();
            state.getRoot().createSnapshot(staging);
            // No REPLACE_EXISTING: a non-empty directory can't be replaced, and target was
            // checked absent above.
            Files.move(staging, target, StandardCopyOption.ATOMIC_MOVE);
            durationMs = (System.nanoTime() - startNanos) / 1_000_000L;
            LOGGER.log(
                    System.Logger.Level.INFO,
                    "Wrote state snapshot for block {0} to {1} in {2} ms",
                    blockNumber,
                    target,
                    durationMs);
        } catch (final IOException | RuntimeException e) {
            LOGGER.log(System.Logger.Level.WARNING, "Failed to write state snapshot for block {0}", blockNumber, e);
            try {
                deleteRecursively(staging);
            } catch (final IOException | RuntimeException cleanup) {
                LOGGER.log(System.Logger.Level.WARNING, "Failed to delete snapshot staging dir {0}", staging, cleanup);
            }
            return new WriteResult(WriteStatus.FAILED, 0L);
        }
        pruneOldRecentSnapshots(blockNumber);
        return new WriteResult(WriteStatus.WRITTEN, durationMs);
    }

    /// Delete every `.tmp-<digits>` staging dir directly under the snapshot root. Only safe
    /// when no snapshot write is in flight. Failures are logged and skipped.
    void deleteStagingDirectories() {
        if (!Files.isDirectory(root)) {
            return;
        }
        final List<Path> staging;
        try (final Stream<Path> entries = Files.list(root)) {
            staging = entries.filter(Files::isDirectory)
                    .filter(p -> isStagingName(p.getFileName().toString()))
                    .toList();
        } catch (final IOException | RuntimeException e) {
            LOGGER.log(System.Logger.Level.WARNING, "Unable to list {0} for stale snapshot staging dirs", root, e);
            return;
        }
        staging.forEach(StateSnapshotManager::deleteQuietly);
    }

    /// Read `BlockStreamInfo.block_number` from `state`'s own singleton: the self-consistency
    /// check that replaces cross-referencing a separate metadata file (§5.1/§6.2).
    ///
    /// @param state the state to read
    /// @return the block number recorded in `state`'s `BlockStreamInfo` singleton, or `-1` if
    ///     absent or unparseable
    static long blockNumberOf(@NonNull final VirtualMapState state) {
        final Bytes raw = state.getSingleton(StateIdentifier.STATE_ID_BLOCK_STREAM_INFO.protoOrdinal());
        if (raw == null || raw.length() == 0L) {
            return -1L;
        }
        try {
            return BlockStreamInfo.PROTOBUF
                    .parse(raw.toReadableSequentialData())
                    .blockNumber();
        } catch (final ParseException e) {
            return -1L;
        }
    }

    /// Scan the snapshot root for numerically-named directories and return the highest one.
    ///
    /// @return the highest-numbered snapshot directory's block number, or `-1` if none exist
    /// @throws IOException if the snapshot root exists but cannot be listed
    private long highestNumberedSnapshotDirectory() throws IOException {
        if (!Files.isDirectory(root)) {
            return -1L;
        }
        try (final var entries = Files.list(root)) {
            return entries.filter(Files::isDirectory)
                    .map(p -> p.getFileName().toString())
                    .filter(StateSnapshotManager::isDigits)
                    .mapToLong(Long::parseLong)
                    .max()
                    .orElse(-1L);
        }
    }

    /// Delete old snapshots so at most `retentionCount` numeric dirs remain, always keeping
    /// `current`, and delete any `.tmp-<digits>` staging dir. Nothing else under the root is
    /// touched: in particular `tmp` holds the live MerkleDb files. Failures are logged and
    /// skipped, since the new snapshot is already written. Runs on the writing thread, so no
    /// other staging dir is in flight.
    ///
    /// @param current the block number of the snapshot just written
    private void pruneOldRecentSnapshots(final long current) {
        final List<Long> others;
        try (final Stream<Path> entries = Files.list(root)) {
            others = entries.filter(Files::isDirectory)
                    .map(p -> p.getFileName().toString())
                    .filter(StateSnapshotManager::isDigits)
                    .map(Long::parseLong)
                    .filter(n -> n != current)
                    .sorted(Comparator.reverseOrder())
                    .toList();
        } catch (final IOException | RuntimeException e) {
            LOGGER.log(System.Logger.Level.WARNING, "Unable to list {0} to prune old snapshots", root, e);
            return;
        }
        final int keepOthers = Math.max(0, retentionCount - 1);
        for (int i = keepOthers; i < others.size(); i++) {
            deleteQuietly(recentSnapshotDirectoryFor(others.get(i)));
        }
        deleteStagingDirectories();
    }

    /// The recent-snapshot directory path for a given block number.
    ///
    /// @param blockNumber the block number
    /// @return the directory path under the snapshot root for that block
    @NonNull
    private Path recentSnapshotDirectoryFor(final long blockNumber) {
        return root.resolve(Long.toString(blockNumber));
    }

    /// @param name a directory's file name
    /// @return `true` if `name` is non-empty and every character is an ASCII digit
    private static boolean isDigits(@NonNull final String name) {
        return !name.isEmpty() && name.chars().allMatch(Character::isDigit);
    }

    /// @param name a directory's file name
    /// @return `true` if `name` is `.tmp-` followed by one or more ASCII digits
    private static boolean isStagingName(@NonNull final String name) {
        return name.startsWith(STAGING_PREFIX) && isDigits(name.substring(STAGING_PREFIX.length()));
    }

    /// Recursively delete `dir`, logging rather than throwing on failure.
    ///
    /// @param dir the directory to delete
    private static void deleteQuietly(@NonNull final Path dir) {
        try {
            deleteRecursively(dir);
            LOGGER.log(System.Logger.Level.DEBUG, "Deleted {0}", dir);
        } catch (final IOException | RuntimeException e) {
            LOGGER.log(System.Logger.Level.WARNING, "Failed to delete {0}", dir, e);
        }
    }

    /// Recursively delete `path` if it exists. Symlinks are not followed.
    ///
    /// @param path the file or directory to delete
    /// @throws IOException if any entry cannot be deleted
    private static void deleteRecursively(@NonNull final Path path) throws IOException {
        if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
            return;
        }
        final List<Path> paths;
        try (final Stream<Path> walk = Files.walk(path)) {
            paths = walk.sorted(Comparator.reverseOrder()).toList();
        }
        for (final Path p : paths) {
            Files.deleteIfExists(p);
        }
    }
}
