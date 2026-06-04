// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.utils;

import com.hedera.hapi.block.stream.output.MapChangeKey;
import com.hedera.hapi.block.stream.output.MapChangeValue;
import com.hedera.hapi.block.stream.output.MapUpdateChange;
import com.hedera.hapi.block.stream.output.QueuePushChange;
import com.hedera.hapi.block.stream.output.SingletonUpdateChange;
import com.hedera.hapi.block.stream.output.StateChange;
import com.hedera.hapi.block.stream.output.StateChanges;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.base.time.Time;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.merkledb.config.MerkleDbConfig;
import com.swirlds.state.BinaryState;
import com.swirlds.state.merkle.VirtualMapState;
import com.swirlds.state.merkle.VirtualMapStateLifecycleManager;
import com.swirlds.virtualmap.config.VirtualMapConfig;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.hiero.base.file.FileSystemManager;
import org.hiero.block.api.StateMetadata;
import org.hiero.consensus.config.PathsConfig;
import org.hiero.consensus.metrics.noop.NoOpMetrics;

/**
 * Produces a minimal, self-contained state snapshot that the {@code state-management-hashgraph}
 * plugin can load on boot, so the E2E suite can exercise real applied-state reads over gRPC
 * (rather than only the {@code NOT_READY} path).
 *
 * <p>The plugin's on-disk snapshot is a pure swirlds artifact: a {@code recent/<blockNumber>/}
 * directory written by {@code VirtualMapStateLifecycleManager.createSnapshot(...)} plus a
 * {@code stateMetadata.json} (PBJ JSON of {@link StateMetadata}) naming that block. On boot the
 * plugin reads the metadata, loads {@code recent/<blockNumber>/}, and exposes it as the
 * network-attested state — no block application is needed. This generator therefore builds the
 * snapshot directly with the same swirlds library and the same PBJ carrier-byte encoding the
 * plugin's {@code StateChangeApplier} uses, so values round-trip through {@code getBinary*}
 * exactly as if they had been streamed in.
 *
 * <p>Why this lives in the suite (not a plugin test-fixture): the plugin reads/writes through the
 * <em>swirlds</em> {@link BinaryState} API, which the suite already has on its module path. Only the
 * "apply blocks through the plugin" path needs the plugin's package-private hash accessor for lag-1
 * footer chaining; writing the snapshot directly does not, so no cross-module wiring is required.
 */
public final class StateSeedSnapshotGenerator {

    /** Singleton state id seeded by {@link #generate(Path)} (read back via {@code getBinarySingleton}). */
    public static final int SINGLETON_STATE_ID = 7;
    /** KV state id seeded by {@link #generate(Path)} (read back via {@code getBinaryKV}). */
    public static final int KV_STATE_ID = 8;
    /** Queue state id seeded by {@link #generate(Path)} (read back via {@code getBinaryQueue}). */
    public static final int QUEUE_STATE_ID = 9;

    /** Raw singleton value bytes carried in the seeded {@link SingletonUpdateChange}. */
    public static final Bytes SINGLETON_VALUE = Bytes.fromHex("c0ffee");
    /** Raw KV key bytes carried in the seeded {@link MapChangeKey}. */
    public static final Bytes KV_KEY_RAW = Bytes.fromHex("ab");
    /** KV value string carried in the seeded {@link MapChangeValue}. */
    public static final String KV_VALUE_STRING = "persisted";
    /** Raw queue element bytes carried in the seeded {@link QueuePushChange}. */
    public static final Bytes QUEUE_ELEMENT = Bytes.fromHex("dd");

    /** Block number the seed snapshot is attested at (also the {@code recent/<n>} directory name). */
    public static final long SEED_BLOCK_NUMBER = 0L;
    /** Round number recorded in the seed metadata. */
    public static final long SEED_ROUND_NUMBER = 10L;

    /**
     * The KV key exactly as the plugin stores it — the PBJ-encoded {@link MapChangeKey}. A
     * {@code getBinaryKV} query must pass these bytes as its {@code keyBytes}.
     */
    public static final Bytes KV_KEY_ENCODED = MapChangeKey.PROTOBUF.toBytes(
            MapChangeKey.newBuilder().protoBytesKey(KV_KEY_RAW).build());

    /** The singleton value exactly as the plugin stores it — PBJ-encoded {@link SingletonUpdateChange}. */
    public static final Bytes SINGLETON_VALUE_ENCODED = SingletonUpdateChange.PROTOBUF.toBytes(
            SingletonUpdateChange.newBuilder().bytesValue(SINGLETON_VALUE).build());

    /** The KV value exactly as the plugin stores it — PBJ-encoded {@link MapChangeValue}. */
    public static final Bytes KV_VALUE_ENCODED = MapChangeValue.PROTOBUF.toBytes(
            MapChangeValue.newBuilder().protoStringValue(KV_VALUE_STRING).build());

    /** The queue element exactly as the plugin stores it — PBJ-encoded {@link QueuePushChange}. */
    public static final Bytes QUEUE_ELEMENT_ENCODED = QueuePushChange.PROTOBUF.toBytes(
            QueuePushChange.newBuilder().protoBytesElement(QUEUE_ELEMENT).build());

    // ── Block 1 (streamed-after-seed) changes — distinct values, so reads confirm the apply ──

    /** Block 1 updates singleton {@link #SINGLETON_STATE_ID} to this new value (distinct from the seed). */
    public static final Bytes BLOCK1_SINGLETON_VALUE = Bytes.fromHex("beef");
    /** Block 1 inserts this new KV key under {@link #KV_STATE_ID} (distinct from the seed's key). */
    public static final Bytes BLOCK1_KV_KEY_RAW = Bytes.fromHex("cd");
    /** Block 1 KV value string. */
    public static final String BLOCK1_KV_VALUE_STRING = "applied";
    /** Block 1 pushes this new element onto queue {@link #QUEUE_STATE_ID}. */
    public static final Bytes BLOCK1_QUEUE_ELEMENT = Bytes.fromHex("ee");

    /** Block 1 singleton value as stored/read — PBJ-encoded {@link SingletonUpdateChange}. */
    public static final Bytes BLOCK1_SINGLETON_VALUE_ENCODED =
            SingletonUpdateChange.PROTOBUF.toBytes(SingletonUpdateChange.newBuilder()
                    .bytesValue(BLOCK1_SINGLETON_VALUE)
                    .build());
    /** Block 1 KV key as stored — the {@code keyBytes} a {@code getBinaryKV} query must pass. */
    public static final Bytes BLOCK1_KV_KEY_ENCODED = MapChangeKey.PROTOBUF.toBytes(
            MapChangeKey.newBuilder().protoBytesKey(BLOCK1_KV_KEY_RAW).build());
    /** Block 1 KV value as stored/read — PBJ-encoded {@link MapChangeValue}. */
    public static final Bytes BLOCK1_KV_VALUE_ENCODED = MapChangeValue.PROTOBUF.toBytes(
            MapChangeValue.newBuilder().protoStringValue(BLOCK1_KV_VALUE_STRING).build());
    /** Block 1 queue element as stored — PBJ-encoded {@link QueuePushChange}. */
    public static final Bytes BLOCK1_QUEUE_ELEMENT_ENCODED = QueuePushChange.PROTOBUF.toBytes(
            QueuePushChange.newBuilder().protoBytesElement(BLOCK1_QUEUE_ELEMENT).build());

    /** Block number that block 1's changes become queryable at (after block 2 attests block 1). */
    public static final long BLOCK1_NUMBER = 1L;
    /** Block number streamed to attest block 1 under lag-1 (carries no state changes). */
    public static final long BLOCK2_NUMBER = 2L;

    /**
     * Result of {@link #generateWithChain(Path)}: the seed snapshot is written to disk, and these
     * footer hashes + block-1 changes let a test stream verifiable blocks the running plugin will apply.
     *
     * @param block1FooterHash the {@code startOfBlockStateRootHash} block 1 must carry — the post-seed
     *     (block 0) state root the plugin will validate against before applying block 1
     * @param block2FooterHash the {@code startOfBlockStateRootHash} block 2 must carry — the post-block-1
     *     state root; delivering block 2 attests block 1, exposing its changes under lag-1
     * @param block1StateChanges the state changes block 1 carries (must equal what was applied to compute
     *     {@code block2FooterHash})
     */
    public record SeededChain(Bytes block1FooterHash, Bytes block2FooterHash, StateChanges block1StateChanges) {}

    private StateSeedSnapshotGenerator() {}

    /**
     * Generate a seed snapshot under {@code stateRoot}, producing
     * {@code stateRoot/recent/<SEED_BLOCK_NUMBER>/} and {@code stateRoot/stateMetadata.json}.
     * Point the plugin's {@code state.management.stateSnapshotRecentPath} at
     * {@code stateRoot/recent} and {@code state.management.stateMetadataPath} at
     * {@code stateRoot/stateMetadata.json} (or a copy thereof) to load it.
     *
     * @param stateRoot the directory to write the snapshot and metadata into
     * @throws Exception if the swirlds state cannot be built or written
     */
    public static void generate(final Path stateRoot) throws Exception {
        final Path recentRoot = stateRoot.resolve("recent");
        final Path metadataPath = stateRoot.resolve("stateMetadata.json");
        Files.createDirectories(recentRoot);

        // Only the swirlds library config records the lifecycle manager reads are needed here.
        final Configuration configuration = ConfigurationBuilder.create()
                .withConfigDataType(MerkleDbConfig.class)
                .withConfigDataType(VirtualMapConfig.class)
                .withConfigDataType(PathsConfig.class)
                .build();

        // Same construction the plugin does in init(): anchor the FileSystemManager scratchpad at
        // the recent-snapshot root.
        final VirtualMapStateLifecycleManager lifecycleManager = new VirtualMapStateLifecycleManager(
                new NoOpMetrics(), Time.getCurrent(), configuration, new FileSystemManager(recentRoot));

        // Initialise a writable mutable (mirrors the plugin's loadPersistedState copyMutableState()).
        lifecycleManager.copyMutableState();

        // Mirror StateChangeApplier exactly: write each change with the same PBJ carrier encoding,
        // so the bytes read back through getBinary* are identical.
        final BinaryState mutable = lifecycleManager.getMutableState();
        mutable.updateSingleton(SINGLETON_STATE_ID, SINGLETON_VALUE_ENCODED);
        mutable.updateKv(KV_STATE_ID, KV_KEY_ENCODED, KV_VALUE_ENCODED);
        mutable.pushQueue(QUEUE_STATE_ID, QUEUE_ELEMENT_ENCODED);

        // Seal the block and snapshot the sealed (attested) state into recent/<block>.
        lifecycleManager.copyMutableState();
        final VirtualMapState sealed = lifecycleManager.getLatestImmutableState();
        final Path snapshotDir = recentRoot.resolve(Long.toString(SEED_BLOCK_NUMBER));
        lifecycleManager.createSnapshot(sealed, snapshotDir);

        // Persist metadata naming the snapshot block (the plugin uses blockNumber to locate the
        // recent/<n> dir; the root hash is re-derived from the loaded state, so it is informational
        // here). Same JSON form StateMetadataStore reads.
        final StateMetadata metadata = StateMetadata.newBuilder()
                .blockNumber(SEED_BLOCK_NUMBER)
                .roundNumber(SEED_ROUND_NUMBER)
                .stateRootHash(rootHashOf(sealed))
                .build();
        Files.write(metadataPath, StateMetadata.JSON.toBytes(metadata).toByteArray());
    }

    /**
     * Generate the seed snapshot (as {@link #generate(Path)}) and additionally compute the footer-hash
     * chain needed to stream blocks 1 and 2 into the running plugin so block 1's changes get applied and
     * exposed. The block-1 changes are applied to the same swirlds state used for the seed, so the
     * returned {@code block2FooterHash} equals the hash the plugin will compute after applying block 1 —
     * letting the streamed block 2 attest block 1 under lag-1.
     *
     * @param stateRoot the directory to write the seed snapshot and metadata into
     * @return the footer-hash chain and the block-1 state changes to stream
     * @throws Exception if the swirlds state cannot be built or written
     */
    public static SeededChain generateWithChain(final Path stateRoot) throws Exception {
        final Path recentRoot = stateRoot.resolve("recent");
        final Path metadataPath = stateRoot.resolve("stateMetadata.json");
        Files.createDirectories(recentRoot);

        final Configuration configuration = ConfigurationBuilder.create()
                .withConfigDataType(MerkleDbConfig.class)
                .withConfigDataType(VirtualMapConfig.class)
                .withConfigDataType(PathsConfig.class)
                .build();
        final VirtualMapStateLifecycleManager lifecycleManager = new VirtualMapStateLifecycleManager(
                new NoOpMetrics(), Time.getCurrent(), configuration, new FileSystemManager(recentRoot));
        lifecycleManager.copyMutableState();

        // Block 0 (seed) — same writes as generate().
        final BinaryState seedMutable = lifecycleManager.getMutableState();
        seedMutable.updateSingleton(SINGLETON_STATE_ID, SINGLETON_VALUE_ENCODED);
        seedMutable.updateKv(KV_STATE_ID, KV_KEY_ENCODED, KV_VALUE_ENCODED);
        seedMutable.pushQueue(QUEUE_STATE_ID, QUEUE_ELEMENT_ENCODED);
        lifecycleManager.copyMutableState();
        final VirtualMapState sealed0 = lifecycleManager.getLatestImmutableState();
        final Bytes block1FooterHash = rootHashOf(sealed0); // post-block-0 root → block 1's footer

        lifecycleManager.createSnapshot(sealed0, recentRoot.resolve(Long.toString(SEED_BLOCK_NUMBER)));
        final StateMetadata metadata = StateMetadata.newBuilder()
                .blockNumber(SEED_BLOCK_NUMBER)
                .roundNumber(SEED_ROUND_NUMBER)
                .stateRootHash(block1FooterHash)
                .build();
        Files.write(metadataPath, StateMetadata.JSON.toBytes(metadata).toByteArray());

        // Block 1 — apply the distinct changes with the SAME encoding StateChangeApplier uses, so the
        // resulting hash matches what the plugin computes when it applies the streamed block 1.
        final BinaryState block1Mutable = lifecycleManager.getMutableState();
        block1Mutable.updateSingleton(SINGLETON_STATE_ID, BLOCK1_SINGLETON_VALUE_ENCODED);
        block1Mutable.updateKv(KV_STATE_ID, BLOCK1_KV_KEY_ENCODED, BLOCK1_KV_VALUE_ENCODED);
        block1Mutable.pushQueue(QUEUE_STATE_ID, BLOCK1_QUEUE_ELEMENT_ENCODED);
        lifecycleManager.copyMutableState();
        final VirtualMapState sealed1 = lifecycleManager.getLatestImmutableState();
        final Bytes block2FooterHash = rootHashOf(sealed1); // post-block-1 root → block 2's footer

        final StateChanges block1StateChanges = StateChanges.newBuilder()
                .stateChanges(List.of(
                        StateChange.newBuilder()
                                .stateId(SINGLETON_STATE_ID)
                                .singletonUpdate(SingletonUpdateChange.newBuilder()
                                        .bytesValue(BLOCK1_SINGLETON_VALUE)
                                        .build())
                                .build(),
                        StateChange.newBuilder()
                                .stateId(KV_STATE_ID)
                                .mapUpdate(MapUpdateChange.newBuilder()
                                        .key(MapChangeKey.newBuilder()
                                                .protoBytesKey(BLOCK1_KV_KEY_RAW)
                                                .build())
                                        .value(MapChangeValue.newBuilder()
                                                .protoStringValue(BLOCK1_KV_VALUE_STRING)
                                                .build())
                                        .build())
                                .build(),
                        StateChange.newBuilder()
                                .stateId(QUEUE_STATE_ID)
                                .queuePush(QueuePushChange.newBuilder()
                                        .protoBytesElement(BLOCK1_QUEUE_ELEMENT)
                                        .build())
                                .build()))
                .build();

        return new SeededChain(block1FooterHash, block2FooterHash, block1StateChanges);
    }

    /**
     * Best-effort root hash of the sealed state, mirroring the plugin's {@code rootHashOf}. The
     * loaded plugin re-derives this from the snapshot, so an empty result here is harmless for reads.
     */
    private static Bytes rootHashOf(final VirtualMapState state) {
        try {
            return Bytes.wrap(state.getRoot().getHash().copyToByteArray());
        } catch (final RuntimeException e) {
            return Bytes.EMPTY;
        }
    }
}
