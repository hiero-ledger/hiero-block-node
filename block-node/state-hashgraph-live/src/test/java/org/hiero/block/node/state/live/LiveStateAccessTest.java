// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.live;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.merkledb.config.MerkleDbConfig;
import com.swirlds.virtualmap.config.VirtualMapConfig;
import org.hiero.consensus.config.PathsConfig;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.webserver.http.HttpService;
import java.nio.file.Path;
import org.hiero.block.api.BinaryStateQuery;
import org.hiero.block.api.BinaryStateQueryResponse;
import org.hiero.block.api.BinaryStateQueryResponse.Code;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.ServiceBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Exercises the gRPC query surface of {@link LiveStatePlugin} (STORY-7).
 * Bypasses Helidon by calling the {@code StateServiceInterface} default
 * methods directly — same code path the gRPC dispatcher invokes on a
 * matched request.
 */
class LiveStateAccessTest {

    @Test
    void kvSingletonAndQueueHappyPaths(@TempDir final Path tmp) {
        final LiveStatePlugin plugin = startPlugin(tmp);

        plugin.lifecycleManager().getMutableState().updateSingleton(1, Bytes.fromHex("aa"));
        plugin.lifecycleManager().getMutableState().updateKv(2, Bytes.fromHex("01"), Bytes.fromHex("bb"));
        plugin.lifecycleManager().getMutableState().pushQueue(3, Bytes.fromHex("aa"));
        plugin.lifecycleManager().getMutableState().pushQueue(3, Bytes.fromHex("bb"));

        final BinaryStateQueryResponse singleton = plugin.getBinarySingleton(
                BinaryStateQuery.newBuilder().stateId(1L).build());
        assertThat(singleton.status()).isEqualTo(Code.SUCCESS);
        assertThat(singleton.singletonBytes()).isEqualTo(Bytes.fromHex("aa"));

        final BinaryStateQueryResponse kv = plugin.getBinaryKV(
                BinaryStateQuery.newBuilder().stateId(2L).keyBytes(Bytes.fromHex("01")).build());
        assertThat(kv.status()).isEqualTo(Code.SUCCESS);
        assertThat(kv.kvBytes()).isEqualTo(Bytes.fromHex("bb"));

        final BinaryStateQueryResponse queueAll = plugin.getBinaryQueue(
                BinaryStateQuery.newBuilder().stateId(3L).build());
        assertThat(queueAll.status()).isEqualTo(Code.SUCCESS);
        assertThat(queueAll.queueBytes()).containsExactly(Bytes.fromHex("aa"), Bytes.fromHex("bb"));

        // Indexed peek uses VirtualMap's internal queue index (head…tail range, not an
        // array index from zero). We assert structural success and that the returned
        // element is one of the pushed values; the exact mapping of index→element is
        // owned by swirlds-state-impl and may differ across versions.
        final BinaryStateQueryResponse queueOne = plugin.getBinaryQueue(
                BinaryStateQuery.newBuilder().stateId(3L).queueIndex(1L).build());
        assertThat(queueOne.status()).isEqualTo(Code.SUCCESS);
        assertThat(queueOne.queueBytes()).hasSize(1);
        assertThat(queueOne.queueBytes().getFirst()).isIn(Bytes.fromHex("aa"), Bytes.fromHex("bb"));

        plugin.stop();
    }

    @Test
    void notFoundAndInvalidShapes(@TempDir final Path tmp) {
        final LiveStatePlugin plugin = startPlugin(tmp);

        // not found — clean state.
        assertThat(plugin.getBinarySingleton(
                        BinaryStateQuery.newBuilder().stateId(99L).build())
                .status())
                .isEqualTo(Code.NOT_FOUND);

        // KV without key.
        assertThat(plugin.getBinaryKV(
                        BinaryStateQuery.newBuilder().stateId(2L).build())
                .status())
                .isEqualTo(Code.INVALID_REQUEST);

        // KV with queue_index set.
        assertThat(plugin.getBinaryKV(BinaryStateQuery.newBuilder()
                        .stateId(2L)
                        .keyBytes(Bytes.fromHex("01"))
                        .queueIndex(1L)
                        .build())
                .status())
                .isEqualTo(Code.INVALID_REQUEST);

        // Singleton with key bytes set.
        assertThat(plugin.getBinarySingleton(BinaryStateQuery.newBuilder()
                        .stateId(1L)
                        .keyBytes(Bytes.fromHex("01"))
                        .build())
                .status())
                .isEqualTo(Code.INVALID_REQUEST);

        // Queue with key bytes set.
        assertThat(plugin.getBinaryQueue(BinaryStateQuery.newBuilder()
                        .stateId(3L)
                        .keyBytes(Bytes.fromHex("01"))
                        .build())
                .status())
                .isEqualTo(Code.INVALID_REQUEST);

        // Stale block_number does not match latest applied.
        assertThat(plugin.getBinarySingleton(BinaryStateQuery.newBuilder()
                        .stateId(1L)
                        .blockNumber(42L)
                        .build())
                .status())
                .isEqualTo(Code.INVALID_REQUEST);

        plugin.stop();
    }

    private static LiveStatePlugin startPlugin(final Path tmp) {
        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final var configuration = ConfigurationBuilder.create()
                .withConfigDataType(LiveStateConfig.class)
                .withConfigDataType(MerkleDbConfig.class)
                .withConfigDataType(VirtualMapConfig.class)
                .withConfigDataType(PathsConfig.class)
                .withValue("state.live.stateMetadataPath",
                        tmp.resolve("md.json").toString())
                .withValue("state.live.stateSnapshotRecentPath",
                        tmp.resolve("recent").toString())
                .withValue("state.live.stateSnapshotHistoricPath",
                        tmp.resolve("historic").toString())
                .withValue("state.live.snapshotIntervalMillis", "3600000")
                .withValue("state.live.stateChangesApplyIntervalMillis", "3600000")
                .build();
        final BlockNodeContext context = new BlockNodeContext(
                configuration, null, null, facility, null, null, null, null, null, null, null);
        final LiveStatePlugin plugin = new LiveStatePlugin();
        plugin.init(context, NOOP_SERVICE_BUILDER);
        plugin.start();
        try {
            plugin.awaitReady(5_000L);
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        return plugin;
    }

    private static final ServiceBuilder NOOP_SERVICE_BUILDER = new ServiceBuilder() {
        @Override
        public void registerHttpService(@NonNull final String path, final HttpService... service) {}

        @Override
        public void registerGrpcService(@NonNull final ServiceInterface service) {}
    };
}
