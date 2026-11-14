// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hiero.block.node.state.management.StateManagementPluginTestSupport.listNames;
import static org.hiero.block.node.state.management.StateManagementPluginTestSupport.numericDirs;
import static org.hiero.block.node.state.management.StateManagementPluginTestSupport.stagingDirs;
import static org.hiero.block.node.state.management.StateManagementPluginTestSupport.writeFile;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.hedera.hapi.block.stream.output.StateIdentifier;
import com.hedera.hapi.node.state.blockstream.BlockStreamInfo;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.base.time.Time;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.merkledb.config.MerkleDbConfig;
import com.swirlds.state.merkle.VirtualMapState;
import com.swirlds.state.merkle.VirtualMapStateLifecycleManager;
import com.swirlds.virtualmap.config.VirtualMapConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import org.hiero.base.crypto.Hash;
import org.hiero.base.file.FileSystemManager;
import org.hiero.block.node.state.management.StateSnapshotManager.Failed;
import org.hiero.block.node.state.management.StateSnapshotManager.FailureKind;
import org.hiero.block.node.state.management.StateSnapshotManager.NoSnapshot;
import org.hiero.block.node.state.management.StateSnapshotManager.RestoreOutcome;
import org.hiero.block.node.state.management.StateSnapshotManager.Restored;
import org.hiero.block.node.state.management.StateSnapshotManager.WriteResult;
import org.hiero.block.node.state.management.StateSnapshotManager.WriteStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/// On-disk snapshot handling in `StateSnapshotManager`, with no plugin and no executors: restore
/// outcomes, the staged write and its rename, retention and prune scope, and the
/// `BlockStreamInfo` self-check read. Uses a real `VirtualMapStateLifecycleManager` on a temp dir.
///
/// `FileSystemManager` wipes `<root>/tmp` when constructed, so each test keeps at most one
/// lifecycle manager alive per root. A restart is modelled by building a fresh one only after
/// the previous one is no longer used.
class StateSnapshotManagerTest {

    private static final int RETENTION = 3;
    private static final int STATE_ID_BLOCK_STREAM_INFO = StateIdentifier.STATE_ID_BLOCK_STREAM_INFO.protoOrdinal();
    private static final int EXTRA_SINGLETON_ID = 1;

    // ── restoreLatest ──────────────────────────────────────────────────────

    @Test
    void restoreLatestFindsNoSnapshotWithoutANumberedDirectory(@TempDir final Path tmp) throws Exception {
        // Anchored outside the roots below so the lifecycle manager does not create them.
        final VirtualMapStateLifecycleManager lifecycle = newLifecycleManager(tmp.resolve("fsm"));

        final Path missing = tmp.resolve("missing");
        assertThat(restore(missing, lifecycle)).as("missing root").isEqualTo(new NoSnapshot());

        final Path empty = Files.createDirectories(tmp.resolve("empty"));
        assertThat(restore(empty, lifecycle)).as("empty root").isEqualTo(new NoSnapshot());

        final Path others = tmp.resolve("others");
        writeFile(others.resolve("tmp").resolve("live"));
        writeFile(others.resolve("notes").resolve("readme.txt"));
        writeFile(others.resolve(".tmp-5").resolve("junk"));
        writeFile(others.resolve("9")); // numeric, but a file rather than a directory
        assertThat(restore(others, lifecycle)).as("only non-snapshot entries").isEqualTo(new NoSnapshot());
    }

    @Test
    void parseBlockNumberAcceptsOnlyCanonicalBlockNumbers() {
        assertThat(StateSnapshotManager.parseBlockNumber("0")).isZero();
        assertThat(StateSnapshotManager.parseBlockNumber("42")).isEqualTo(42L);
        assertThat(StateSnapshotManager.parseBlockNumber(Long.toString(Long.MAX_VALUE)))
                .isEqualTo(Long.MAX_VALUE);

        final List<String> notSnapshotNames = List.of(
                "",
                "007", // leading zeros: <root>/7 is a different directory
                "00",
                "9223372036854775808", // Long.MAX_VALUE + 1
                "99999999999999999999", // 20 digits
                "٣", // ARABIC-INDIC DIGIT THREE: Character.isDigit, but not ASCII
                "1٢",
                "12a",
                "-1",
                "+1",
                " 1");
        for (final String name : notSnapshotNames) {
            assertThat(StateSnapshotManager.parseBlockNumber(name))
                    .as("'%s' is not a snapshot directory name", name)
                    .isEqualTo(-1L);
        }
    }

    @Test
    void restoreAndPruneLeaveNonCanonicalNumericNamesAlone(@TempDir final Path tmp) throws Exception {
        final Path root = tmp.resolve("recent");
        final VirtualMapStateLifecycleManager seeder = newLifecycleManager(tmp.resolve("seed"));
        final Hash hash2 = seedSnapshot(seeder, root.resolve("2"), 2L);
        // Each of these used to break restore or prune: the 20-digit name threw out of the
        // scan, and the others parsed to a number whose directory is a different path.
        final List<Path> nonCanonical = List.of(
                writeFile(root.resolve("99999999999999999999").resolve("marker")),
                writeFile(root.resolve("٣").resolve("marker")),
                writeFile(root.resolve("0005").resolve("marker")),
                writeFile(root.resolve(".tmp-007").resolve("marker")));

        final VirtualMapStateLifecycleManager lifecycle = newLifecycleManager(root);
        final StateSnapshotManager manager = new StateSnapshotManager(root, 1, lifecycle);
        assertThat(manager.restoreLatest())
                .as("restore ignores non-canonical names instead of throwing or loading the wrong dir")
                .isEqualTo(new Restored(2L, hash2, root.resolve("2")));

        // Retention 1: each write prunes every other snapshot dir and every staging dir.
        assertThat(write(manager, sealState(lifecycle, 3L), 3L).status()).isEqualTo(WriteStatus.WRITTEN);
        assertThat(write(manager, sealState(lifecycle, 4L), 4L).status()).isEqualTo(WriteStatus.WRITTEN);
        manager.deleteStagingDirectories();

        assertThat(numericDirs(root)).containsExactly(4L);
        for (final Path marker : nonCanonical) {
            assertThat(marker)
                    .as("%s is not a snapshot or staging dir, so it is never deleted", marker.getParent())
                    .exists();
        }
    }

    @Test
    void restoreLatestLoadsTheHighestNumberedSnapshot(@TempDir final Path tmp) throws Exception {
        final Path root = tmp.resolve("recent");
        final VirtualMapStateLifecycleManager seeder = newLifecycleManager(tmp.resolve("seed"));
        seedSnapshot(seeder, root.resolve("3"), 3L);
        // "12" sorts before "7" as a string: the pick must be numeric.
        final Hash hash12 = seedSnapshot(seeder, root.resolve("12"), 12L);
        seedSnapshot(seeder, root.resolve("7"), 7L);
        writeFile(root.resolve(".tmp-99").resolve("junk"));

        final VirtualMapStateLifecycleManager restorer = newLifecycleManager(root);
        assertThat(new StateSnapshotManager(root, RETENTION, restorer).restoreLatest())
                .isEqualTo(new Restored(12L, hash12, root.resolve("12")));
        assertThat(restorer.getLatestImmutableState().getSingleton(EXTRA_SINGLETON_ID))
                .as("block 12's content was loaded into the lifecycle manager")
                .isEqualTo(extraValue(12L));
    }

    @Test
    void restoreLatestReportsAnUnreadableSnapshotWithoutFallingBack(@TempDir final Path tmp) throws Exception {
        final Path root = tmp.resolve("recent");
        seedSnapshot(newLifecycleManager(tmp.resolve("seed")), root.resolve("3"), 3L);
        // Exists, so restore must try it, but holds no snapshot content.
        Files.createDirectories(root.resolve("5"));

        final RestoreOutcome outcome = restore(root, newLifecycleManager(root));

        assertThat(outcome).isInstanceOfSatisfying(Failed.class, failed -> {
            assertThat(failed.kind()).isEqualTo(FailureKind.UNREADABLE);
            assertThat(failed.directory())
                    .as("the highest dir is reported; the older valid snapshot is not used instead")
                    .isEqualTo(root.resolve("5"));
            assertThat(failed.expectedBlock()).isEqualTo(5L);
            assertThat(failed.foundBlock()).isEqualTo(-1L);
            assertThat(failed.cause()).isNotNull();
        });
    }

    @Test
    void restoreLatestReportsASelfCheckMismatch(@TempDir final Path tmp) throws Exception {
        final Path root = tmp.resolve("recent");
        // The state records block 7 but sits in directory "5".
        seedSnapshot(newLifecycleManager(tmp.resolve("seed")), root.resolve("5"), 7L);

        assertThat(restore(root, newLifecycleManager(root)))
                .isEqualTo(new Failed(FailureKind.SELF_CHECK_MISMATCH, root.resolve("5"), 5L, 7L, null));
    }

    @Test
    void restoreLatestReportsAnUnlistableRoot(@TempDir final Path tmp) throws Exception {
        final Path root = tmp.resolve("recent");
        final VirtualMapStateLifecycleManager lifecycle = newLifecycleManager(root);
        try {
            // Exists, so snapshots may be inside, but cannot be listed. Skipped where permissions
            // are not enforced (e.g. running as root, non-POSIX filesystems).
            Files.setPosixFilePermissions(root, PosixFilePermissions.fromString("-wx------"));
            assumeTrue(!Files.isReadable(root), "directory permissions are not enforced here");

            assertThat(restore(root, lifecycle)).isInstanceOfSatisfying(Failed.class, failed -> {
                assertThat(failed.kind()).isEqualTo(FailureKind.UNLISTABLE_ROOT);
                assertThat(failed.directory()).isNull();
                assertThat(failed.expectedBlock()).isEqualTo(-1L);
                assertThat(failed.foundBlock()).isEqualTo(-1L);
                assertThat(failed.cause()).isInstanceOf(IOException.class);
            });
        } finally {
            Files.setPosixFilePermissions(root, PosixFilePermissions.fromString("rwx------"));
        }
    }

    // ── write ──────────────────────────────────────────────────────────────

    @Test
    void writePublishesACompleteSnapshotThatRestoresToTheSameState(@TempDir final Path tmp) throws Exception {
        final Path root = tmp.resolve("recent");
        final VirtualMapStateLifecycleManager writer = newLifecycleManager(root);
        final Path liveMarker = writeFile(root.resolve("tmp").resolve("live-marker"));
        final VirtualMapState state = sealState(writer, 4L);
        final Hash writtenHash = state.getRoot().getHash();

        final WriteResult result = write(new StateSnapshotManager(root, RETENTION, writer), state, 4L);

        assertThat(result.status()).isEqualTo(WriteStatus.WRITTEN);
        assertThat(result.durationMs()).isNotNegative();
        assertThat(numericDirs(root)).containsExactly(4L);
        assertThat(listNames(root.resolve("4"))).isNotEmpty();
        assertThat(stagingDirs(root)).as("the staging dir was renamed").isEmpty();
        assertThat(liveMarker).as("the live MerkleDb dir is left alone").exists();

        // Restart: the writer is no longer used, so a fresh lifecycle manager may take the root.
        final VirtualMapStateLifecycleManager restorer = newLifecycleManager(root);
        assertThat(new StateSnapshotManager(root, RETENTION, restorer).restoreLatest())
                .isEqualTo(new Restored(4L, writtenHash, root.resolve("4")));
        assertThat(restorer.getLatestImmutableState().getSingleton(EXTRA_SINGLETON_ID))
                .isEqualTo(extraValue(4L));
    }

    @Test
    void writeKeepsAnExistingSnapshotAndReplacesAStaleStagingDir(@TempDir final Path tmp) throws Exception {
        final Path root = tmp.resolve("recent");
        final VirtualMapStateLifecycleManager writer = newLifecycleManager(root);
        final StateSnapshotManager manager = new StateSnapshotManager(root, RETENTION, writer);
        final Path marker = writeFile(root.resolve("1").resolve("marker"));
        final Path unrelatedStaging = writeFile(root.resolve(".tmp-9").resolve("junk"));

        assertThat(write(manager, sealState(writer, 1L), 1L))
                .as("an existing <N> dir is treated as complete")
                .isEqualTo(new WriteResult(WriteStatus.ALREADY_PRESENT, 0L));
        assertThat(listNames(root.resolve("1")))
                .as("the existing dir is not overwritten")
                .containsExactly(marker.getFileName().toString());
        assertThat(unrelatedStaging)
                .as("nothing is pruned unless a snapshot was written")
                .exists();

        writeFile(root.resolve(".tmp-2").resolve("junk"));
        final VirtualMapState state2 = sealState(writer, 2L);
        final Hash hash2 = state2.getRoot().getHash();
        assertThat(write(manager, state2, 2L).status()).isEqualTo(WriteStatus.WRITTEN);
        assertThat(root.resolve("2").resolve("junk"))
                .as("the stale staging content was cleared before the write")
                .doesNotExist();
        assertThat(stagingDirs(root)).isEmpty();
        assertThat(numericDirs(root)).containsExactly(1L, 2L);

        assertThat(restore(root, newLifecycleManager(root)))
                .as("the snapshot written over a stale staging dir is loadable")
                .isEqualTo(new Restored(2L, hash2, root.resolve("2")));
    }

    @Test
    void writeFailureLeavesNothingBehindAndSucceedsOnceWritable(@TempDir final Path tmp) throws Exception {
        final Path root = tmp.resolve("recent");
        final VirtualMapStateLifecycleManager writer = newLifecycleManager(root);
        final StateSnapshotManager manager = new StateSnapshotManager(root, RETENTION, writer);
        final VirtualMapState state = sealState(writer, 1L);
        try {
            // Root is listable but nothing can be created in it. The live MerkleDb under tmp
            // keeps its own permissions. Skipped where permissions are not enforced.
            Files.setPosixFilePermissions(root, PosixFilePermissions.fromString("r-x------"));
            assumeTrue(!Files.isWritable(root), "directory permissions are not enforced here");

            assertThat(write(manager, state, 1L)).isEqualTo(new WriteResult(WriteStatus.FAILED, 0L));
            assertThat(numericDirs(root)).isEmpty();
            assertThat(stagingDirs(root)).isEmpty();
        } finally {
            Files.setPosixFilePermissions(root, PosixFilePermissions.fromString("rwx------"));
        }

        assertThat(write(manager, state, 1L).status())
                .as("the same state writes once the root is writable again")
                .isEqualTo(WriteStatus.WRITTEN);
        assertThat(numericDirs(root)).containsExactly(1L);
    }

    @Test
    void writePrunesOnlySnapshotEntriesBeyondRetention(@TempDir final Path tmp) throws Exception {
        final Path root = tmp.resolve("recent");
        final VirtualMapStateLifecycleManager writer = newLifecycleManager(root);
        final StateSnapshotManager manager = new StateSnapshotManager(root, RETENTION, writer);
        // Not snapshot entries: must survive every prune.
        final Path liveMarker = writeFile(root.resolve("tmp").resolve("live-marker"));
        final Path notes = writeFile(root.resolve("notes").resolve("readme.txt"));
        final Path nonDigitStaging = writeFile(root.resolve(".tmp-keep").resolve("file"));

        for (long block = 1L; block <= 5L; block++) {
            if (block == 5L) {
                // Appears mid-run: removed by the prune that follows the next write.
                writeFile(root.resolve(".tmp-77").resolve("junk"));
            }
            assertThat(write(manager, sealState(writer, block), block).status())
                    .as("snapshot of block %d", block)
                    .isEqualTo(WriteStatus.WRITTEN);
        }

        assertThat(numericDirs(root)).containsExactly(3L, 4L, 5L);
        assertThat(stagingDirs(root)).isEmpty();
        assertThat(notes).exists();
        assertThat(nonDigitStaging).as("only .tmp-<digits> counts as staging").exists();
        assertThat(liveMarker).as("the live MerkleDb dir is never pruned").exists();

        // The live state still seals and writes after all that pruning.
        assertThat(write(manager, sealState(writer, 6L), 6L).status()).isEqualTo(WriteStatus.WRITTEN);
        assertThat(numericDirs(root)).containsExactly(4L, 5L, 6L);
    }

    @Test
    void deleteStagingDirectoriesOnlyRemovesDotTmpDigitDirectories(@TempDir final Path tmp) throws Exception {
        final VirtualMapStateLifecycleManager lifecycle = newLifecycleManager(tmp.resolve("fsm"));
        new StateSnapshotManager(tmp.resolve("missing"), RETENTION, lifecycle).deleteStagingDirectories();
        assertThat(tmp.resolve("missing")).as("a missing root is not created").doesNotExist();

        final Path root = tmp.resolve("recent");
        writeFile(root.resolve(".tmp-3").resolve("junk"));
        writeFile(root.resolve(".tmp-12").resolve("nested").resolve("junk"));
        writeFile(root.resolve(".tmp-keep").resolve("file"));
        writeFile(root.resolve(".tmp-").resolve("file"));
        writeFile(root.resolve(".tmp-4")); // a file, not a directory
        writeFile(root.resolve("7").resolve("snapshot"));
        writeFile(root.resolve("tmp").resolve("live"));
        writeFile(root.resolve("notes").resolve("readme.txt"));

        new StateSnapshotManager(root, RETENTION, lifecycle).deleteStagingDirectories();

        assertThat(listNames(root)).containsExactly(".tmp-", ".tmp-4", ".tmp-keep", "7", "notes", "tmp");
        assertThat(root.resolve("7").resolve("snapshot")).exists();
    }

    // ── blockNumberOf ──────────────────────────────────────────────────────

    @Test
    void blockNumberOfReadsBlockStreamInfoOrReportsMinusOne(@TempDir final Path tmp) {
        final VirtualMapState state = newLifecycleManager(tmp.resolve("fsm")).getMutableState();

        assertThat(StateSnapshotManager.blockNumberOf(state)).as("absent").isEqualTo(-1L);

        state.updateSingleton(STATE_ID_BLOCK_STREAM_INFO, blockStreamInfo(42L));
        assertThat(StateSnapshotManager.blockNumberOf(state)).as("present").isEqualTo(42L);

        // Field 1 as a varint whose continuation bit promises a byte that never comes.
        state.updateSingleton(STATE_ID_BLOCK_STREAM_INFO, Bytes.fromHex("0880"));
        assertThat(StateSnapshotManager.blockNumberOf(state)).as("unparseable").isEqualTo(-1L);
    }

    // ── Fixtures ───────────────────────────────────────────────────────────

    /// A lifecycle manager whose `FileSystemManager` is anchored at `fsmRoot`, with the live
    /// mutable already a writable copy (as the plugin does at start-up). Wipes `<fsmRoot>/tmp`.
    private static VirtualMapStateLifecycleManager newLifecycleManager(final Path fsmRoot) {
        final var config = ConfigurationBuilder.create()
                .withConfigDataType(MerkleDbConfig.class)
                .withConfigDataType(VirtualMapConfig.class)
                .build();
        final var lifecycle = new VirtualMapStateLifecycleManager(
                new NoOpSwirldsMetrics(), Time.getCurrent(), config, new FileSystemManager(fsmRoot));
        lifecycle.copyMutableState();
        return lifecycle;
    }

    /// Record `recordedBlock` in `BlockStreamInfo` plus a per-block extra singleton, then seal
    /// and hash the state, like the plugin does after applying a block.
    ///
    /// @return the sealed, hashed state (the lifecycle manager's latest immutable state)
    private static VirtualMapState sealState(
            final VirtualMapStateLifecycleManager lifecycle, final long recordedBlock) {
        final VirtualMapState mutable = lifecycle.getMutableState();
        mutable.updateSingleton(STATE_ID_BLOCK_STREAM_INFO, blockStreamInfo(recordedBlock));
        mutable.updateSingleton(EXTRA_SINGLETON_ID, extraValue(recordedBlock));
        lifecycle.copyMutableState();
        final VirtualMapState sealed = lifecycle.getLatestImmutableState();
        sealed.getRoot().getHash(); // block until hashed; a snapshot needs a hashed copy
        return sealed;
    }

    /// Write `state` through the manager the way the plugin does: holding a reservation on its
    /// root for the duration of the call.
    private static WriteResult write(
            final StateSnapshotManager manager, final VirtualMapState state, final long blockNumber) {
        state.getRoot().reserve();
        try {
            return manager.write(state, blockNumber);
        } finally {
            state.getRoot().release();
        }
    }

    /// Seed `dir` with a snapshot of a state recording `recordedBlock`, using the lifecycle
    /// manager's own writer so restore is tested independently of `StateSnapshotManager.write`.
    ///
    /// @return the seeded state's root hash
    private static Hash seedSnapshot(
            final VirtualMapStateLifecycleManager seeder, final Path dir, final long recordedBlock) {
        final VirtualMapState sealed = sealState(seeder, recordedBlock);
        seeder.createSnapshot(sealed, dir);
        // The lifecycle manager only logs write errors, so check the seed landed.
        assertThat(dir).isDirectory();
        return sealed.getRoot().getHash();
    }

    private static RestoreOutcome restore(final Path root, final VirtualMapStateLifecycleManager lifecycle) {
        return new StateSnapshotManager(root, RETENTION, lifecycle).restoreLatest();
    }

    private static Bytes blockStreamInfo(final long blockNumber) {
        return BlockStreamInfo.PROTOBUF.toBytes(
                BlockStreamInfo.newBuilder().blockNumber(blockNumber).build());
    }

    private static Bytes extraValue(final long blockNumber) {
        return Bytes.wrap(("value-" + blockNumber).getBytes(StandardCharsets.UTF_8));
    }
}
