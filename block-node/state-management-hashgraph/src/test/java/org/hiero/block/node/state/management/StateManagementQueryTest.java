// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.block.stream.input.RoundHeader;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.block.stream.output.SingletonUpdateChange;
import com.hedera.hapi.block.stream.output.StateChange;
import com.hedera.hapi.block.stream.output.StateChanges;
import com.hedera.hapi.block.stream.output.StateIdentifier;
import com.hedera.hapi.node.state.blockstream.BlockStreamInfo;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.base.time.Time;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.merkledb.config.MerkleDbConfig;
import com.swirlds.state.merkle.VirtualMapState;
import com.swirlds.state.merkle.VirtualMapStateLifecycleManager;
import com.swirlds.virtualmap.config.VirtualMapConfig;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.hiero.base.file.FileSystemManager;
import org.hiero.block.api.StateQuery;
import org.hiero.block.api.StateResponse;
import org.hiero.block.api.StateResponse.Code;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.plugintest.RecordingServiceBuilder;
import org.hiero.block.node.app.fixtures.plugintest.RecordingServiceBuilder.GrpcServiceRegistration;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.core.MetricRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/// `StateService` queries through a real `StateManagementPlugin`: registration, restore, lag-1,
/// halt and the inert plugin.
class StateManagementQueryTest {

    private static final int SINGLETON_ID = 7;
    private static final Bytes SEEDED_VALUE = Bytes.fromHex("c0ffee");
    private static final Bytes STREAMED_VALUE = Bytes.fromHex("beef");
    /// What the applier stores for `STREAMED_VALUE`: the oneof payload, a `BytesValue` wrapper.
    private static final Bytes STREAMED_STORED = Bytes.fromHex("0a02beef");

    /// A started plugin with the collaborators tests assert on.
    private record Running(
            StateManagementPlugin plugin,
            TestBlockMessagingFacility facility,
            RecordingServiceBuilder serviceBuilder,
            MetricRegistry metrics) {}

    @Test
    void registersTheStateServiceOnTheSharedPortByDefaultAndOnTheConfiguredPort(@TempDir final Path tmp) {
        final RecordingServiceBuilder shared = new RecordingServiceBuilder();
        final StateManagementPlugin sharedPlugin = new StateManagementPlugin();
        sharedPlugin.init(context(tmp.resolve("a"), null), shared);

        final RecordingServiceBuilder dedicated = new RecordingServiceBuilder();
        final StateManagementPlugin dedicatedPlugin = new StateManagementPlugin();
        dedicatedPlugin.init(context(tmp.resolve("b"), 40_999), dedicated);

        assertThat(shared.grpcServiceRegistrations()).containsExactly(new GrpcServiceRegistration(null, sharedPlugin));
        assertThat(dedicated.grpcServiceRegistrations())
                .containsExactly(new GrpcServiceRegistration(40_999, dedicatedPlugin));
        sharedPlugin.stop();
        dedicatedPlugin.stop();
    }

    @Test
    void restoredSnapshotIsServedBeforeAnyNewBlock(@TempDir final Path tmp) throws Exception {
        final Path recentRoot = tmp.resolve("recent");
        seedSnapshot(recentRoot, 5L);
        final Running running = start(recentRoot);
        final StateManagementPlugin plugin = running.plugin();

        final StateResponse latest = plugin.getSingleton(latest(SINGLETON_ID));
        assertThat(latest.status()).isEqualTo(Code.SUCCESS);
        assertThat(latest.blockNumber()).isEqualTo(5L);
        assertThat(latest.singletonBytes()).isEqualTo(SEEDED_VALUE);
        assertThat(plugin.getSingleton(at(5L, SINGLETON_ID)).status()).isEqualTo(Code.SUCCESS);
        assertThat(plugin.getSingleton(at(6L, SINGLETON_ID)).status()).isEqualTo(Code.NOT_AVAILABLE);

        // The plugin's delegates are wired to the registered state_query_total counter.
        final LongCounter counter = running.metrics().getMetric(StateManagementPlugin.METRIC_QUERY_TOTAL);
        assertThat(counter.getOrCreateLabeled(
                                StateQueryHandler.LABEL_KIND, "singleton", StateQueryHandler.LABEL_STATUS, "success")
                        .get())
                .isEqualTo(2L);
        plugin.stop();
    }

    @Test
    void lagOneServesTheBlockBeforeTheLatestApplied(@TempDir final Path tmp) throws Exception {
        final Running running = start(tmp.resolve("recent"));
        final StateManagementPlugin plugin = running.plugin();

        deliver(running, buildBlock(0L, plugin.stagedStateRootHash(), STREAMED_VALUE));
        // Block 0 is staged but not attested until block 1's footer confirms it.
        final StateResponse beforeAttest = plugin.getSingleton(latest(SINGLETON_ID));
        assertThat(beforeAttest.status()).isEqualTo(Code.NOT_READY);
        assertThat(beforeAttest.blockNumber()).isEqualTo(StateQueryHandler.NOT_READY_BLOCK_NUMBER);

        deliver(running, buildBlock(1L, plugin.stagedStateRootHash(), null));
        assertThat(plugin.isApplyHalted()).isFalse();

        final StateResponse latest = plugin.getSingleton(latest(SINGLETON_ID));
        assertThat(latest.status()).isEqualTo(Code.SUCCESS);
        assertThat(latest.blockNumber()).isZero();
        assertThat(latest.singletonBytes()).isEqualTo(STREAMED_STORED);
        assertThat(plugin.getSingleton(at(0L, SINGLETON_ID)).status()).isEqualTo(Code.SUCCESS);
        final StateResponse applied = plugin.getSingleton(at(1L, SINGLETON_ID));
        assertThat(applied.status()).isEqualTo(Code.NOT_AVAILABLE);
        assertThat(applied.blockNumber()).isZero();
        plugin.stop();
    }

    @Test
    void haltedNodeKeepsServingItsFrozenAttestedBlock(@TempDir final Path tmp) throws Exception {
        final Path recentRoot = tmp.resolve("recent");
        seedSnapshot(recentRoot, 5L);
        final Running running = start(recentRoot);
        final StateManagementPlugin plugin = running.plugin();

        deliver(running, buildBlock(6L, Bytes.fromHex("deadbeef".repeat(8)), STREAMED_VALUE));
        assertThat(plugin.isApplyHalted()).isTrue();

        final StateResponse response = plugin.getSingleton(latest(SINGLETON_ID));
        assertThat(response.status()).isEqualTo(Code.SUCCESS);
        assertThat(response.blockNumber()).isEqualTo(5L);
        assertThat(response.singletonBytes()).isEqualTo(SEEDED_VALUE);
        plugin.stop();
    }

    @Test
    void inertPluginStillRegistersAndAnswersNotReady(@TempDir final Path tmp) throws Exception {
        // Parent is a regular file, so the state directory can't be created: the plugin stays inert.
        final Path blocker = tmp.resolve("blocker");
        Files.writeString(blocker, "not a directory");
        final RecordingServiceBuilder serviceBuilder = new RecordingServiceBuilder();
        final StateManagementPlugin plugin = new StateManagementPlugin();
        plugin.init(context(blocker.resolve("state/recent"), null), serviceBuilder);
        plugin.start();

        assertThat(serviceBuilder.grpcServiceRegistrations()).hasSize(1);
        final List<StateResponse> responses = new ArrayList<>();
        responses.add(plugin.getKV(StateQuery.newBuilder()
                .retrieveLatest(true)
                .stateId(8L)
                .keyBytes(Bytes.fromHex("01"))
                .build()));
        responses.add(plugin.getSingleton(latest(SINGLETON_ID)));
        responses.add(plugin.getQueue(latest(10L)));
        assertThat(responses).allSatisfy(response -> {
            assertThat(response.status()).isEqualTo(Code.NOT_READY);
            assertThat(response.blockNumber()).isEqualTo(StateQueryHandler.NOT_READY_BLOCK_NUMBER);
        });
        plugin.stop();
    }

    // ── Fixtures ───────────────────────────────────────────────────────────

    /// Write `recentRoot/<blockNumber>` holding the test singleton and a matching `BlockStreamInfo`.
    private static void seedSnapshot(final Path recentRoot, final long blockNumber) throws Exception {
        Files.createDirectories(recentRoot);
        final var config = ConfigurationBuilder.create()
                .withConfigDataType(MerkleDbConfig.class)
                .withConfigDataType(VirtualMapConfig.class)
                .build();
        final var manager = new VirtualMapStateLifecycleManager(
                new NoOpSwirldsMetrics(), Time.getCurrent(), config, new FileSystemManager(recentRoot));
        manager.copyMutableState();
        final VirtualMapState mutable = manager.getMutableState();
        mutable.updateSingleton(SINGLETON_ID, SEEDED_VALUE);
        mutable.updateSingleton(
                StateIdentifier.STATE_ID_BLOCK_STREAM_INFO.protoOrdinal(),
                BlockStreamInfo.PROTOBUF.toBytes(
                        BlockStreamInfo.newBuilder().blockNumber(blockNumber).build()));
        manager.copyMutableState();
        final VirtualMapState sealed = manager.getLatestImmutableState();
        sealed.getRoot().getHash();
        manager.createSnapshot(sealed, recentRoot.resolve(Long.toString(blockNumber)));
    }

    private static Running start(final Path recentRoot) throws InterruptedException {
        final MetricRegistry metrics = MetricRegistry.builder().build();
        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final RecordingServiceBuilder serviceBuilder = new RecordingServiceBuilder();
        final StateManagementPlugin plugin = new StateManagementPlugin();
        plugin.init(context(recentRoot, null, metrics, facility), serviceBuilder);
        plugin.start();
        StateManagementPluginTestSupport.awaitReady(plugin, 5_000L);
        return new Running(plugin, facility, serviceBuilder, metrics);
    }

    private static BlockNodeContext context(final Path recentRoot, @Nullable final Integer port) {
        return context(recentRoot, port, MetricRegistry.builder().build(), new TestBlockMessagingFacility());
    }

    private static BlockNodeContext context(
            final Path recentRoot,
            @Nullable final Integer port,
            final MetricRegistry metrics,
            final TestBlockMessagingFacility facility) {
        final var builder = ConfigurationBuilder.create()
                .withConfigDataType(StateManagementConfig.class)
                .withConfigDataType(MerkleDbConfig.class)
                .withConfigDataType(VirtualMapConfig.class)
                .withValue("state.management.snapshotRecentPath", recentRoot.toString());
        if (port != null) {
            builder.withValue("state.management.port", port.toString());
        }
        return new BlockNodeContext(
                builder.build(),
                metrics,
                null,
                facility,
                null,
                null,
                null,
                StateManagementPluginTestSupport.newThreadPoolManager(),
                null);
    }

    private static void deliver(final Running running, final BlockUnparsed block) throws Exception {
        final long blockNumber = BlockHeader.PROTOBUF
                .parse(block.blockItems().getFirst().blockHeaderOrThrow())
                .number();
        running.facility()
                .sendBlockVerification(new VerificationNotification(
                        true, null, blockNumber, Bytes.fromHex("aabb"), block, BlockSource.PUBLISHER));
        running.plugin().applyPending();
    }

    /// A block whose footer carries `startHash`, optionally setting the test singleton to `value`.
    private static BlockUnparsed buildBlock(
            final long blockNumber, final Bytes startHash, @Nullable final Bytes value) {
        final List<BlockItemUnparsed> items = new ArrayList<>();
        items.add(BlockItemUnparsed.newBuilder()
                .blockHeader(BlockHeader.PROTOBUF.toBytes(
                        BlockHeader.newBuilder().number(blockNumber).build()))
                .build());
        items.add(BlockItemUnparsed.newBuilder()
                .roundHeader(RoundHeader.PROTOBUF.toBytes(
                        RoundHeader.newBuilder().roundNumber(blockNumber * 10L).build()))
                .build());
        if (value != null) {
            items.add(BlockItemUnparsed.newBuilder()
                    .stateChanges(StateChanges.PROTOBUF.toBytes(StateChanges.newBuilder()
                            .stateChanges(StateChange.newBuilder()
                                    .stateId(SINGLETON_ID)
                                    .singletonUpdate(SingletonUpdateChange.newBuilder()
                                            .bytesValue(value)
                                            .build())
                                    .build())
                            .build()))
                    .build());
        }
        items.add(BlockItemUnparsed.newBuilder()
                .blockFooter(BlockFooter.PROTOBUF.toBytes(BlockFooter.newBuilder()
                        .startOfBlockStateRootHash(startHash)
                        .build()))
                .build());
        return BlockUnparsed.newBuilder().blockItems(items).build();
    }

    private static StateQuery latest(final long stateId) {
        return StateQuery.newBuilder().retrieveLatest(true).stateId(stateId).build();
    }

    private static StateQuery at(final long blockNumber, final long stateId) {
        return StateQuery.newBuilder().blockNumber(blockNumber).stateId(stateId).build();
    }
}
