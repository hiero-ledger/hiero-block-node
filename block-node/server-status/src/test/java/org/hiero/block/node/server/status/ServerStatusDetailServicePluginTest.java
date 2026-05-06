// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.server.status;

import static org.hiero.block.node.app.fixtures.TestUtils.enableDebugLogging;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import org.hiero.block.api.BlockNodeVersions;
import org.hiero.block.api.BlockNodeVersions.PluginVersion;
import org.hiero.block.api.BlockRange;
import org.hiero.block.api.ServerStatusDetailResponse;
import org.hiero.block.api.ServerStatusRequest;
import org.hiero.block.api.TssData;
import org.hiero.block.api.TssRoster;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.plugintest.GrpcPluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleBlockRangeSet;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.module.SemanticVersionUtility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for the ServerStatusServicePlugin class.
 * Validates the functionality of the server status service and its responses
 * under different conditions.
 */
public class ServerStatusDetailServicePluginTest
        extends GrpcPluginTestBase<ServerStatusServicePlugin, BlockingExecutor, ScheduledExecutorService> {
    private final ServerStatusServicePlugin plugin = new ServerStatusServicePlugin();

    public ServerStatusDetailServicePluginTest() {
        super(
                new BlockingExecutor(new LinkedBlockingQueue<>()),
                new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        final SimpleInMemoryHistoricalBlockFacility historicalBlockFacility =
                new SimpleInMemoryHistoricalBlockFacility();
        final SimpleBlockRangeSet temporaryAvailableBlocks = new SimpleBlockRangeSet();
        temporaryAvailableBlocks.add(1_000_000_000_000L, 1_000_000_000_005L);
        temporaryAvailableBlocks.add(0L, 5L);
        temporaryAvailableBlocks.add(1_000_000_000L, 1_000_000_005L);
        temporaryAvailableBlocks.add(1_000_000L, 1_000_005L);

        historicalBlockFacility.setTemporaryAvailableBlocks(temporaryAvailableBlocks);
        start(plugin, plugin.methods().getLast(), historicalBlockFacility);
    }

    /**
     * Enable debug logging for each test.
     */
    @BeforeEach
    void setup() {
        enableDebugLogging();
    }

    /**
     * Tests that the server status detail response is valid when requested. Verifies the block node version and the
     * plugin versions.
     *
     * @throws ParseException if there is an error parsing the response
     */
    @Test
    @DisplayName("Should return valid Server Detail Status when requested")
    void shouldReturnValidServerStatus() throws ParseException {
        final ServerStatusRequest request = ServerStatusRequest.newBuilder().build();
        toPluginPipe.onNext(ServerStatusRequest.PROTOBUF.toBytes(request));
        assertEquals(1, fromPluginBytes.size());

        final ServerStatusDetailResponse response =
                ServerStatusDetailResponse.PROTOBUF.parse(fromPluginBytes.getFirst());

        final List<BlockRange> blockRanges = response.availableRanges();
        assertFalse(blockRanges.isEmpty());
        assertEquals(4, blockRanges.size());

        BlockRange blockRange = blockRanges.getFirst();
        // make sure block ranges are ordered.
        assertEquals(0L, blockRange.rangeStart());
        assertEquals(5L, blockRange.rangeEnd());
        blockRange = blockRanges.getLast();
        // make sure block ranges are ordered.
        assertEquals(1_000_000_000_000L, blockRange.rangeStart());
        assertEquals(1_000_000_000_005L, blockRange.rangeEnd());

        assertNotNull(response);
        assertTrue(response.hasVersionInformation());
        final BlockNodeVersions blockNodeVersions = response.versionInformation();
        assertNotNull(blockNodeVersions);
        final SemanticVersion semanticVersion = SemanticVersionUtility.from(this.getClass());
        assertEquals(semanticVersion, blockNodeVersions.blockNodeVersion());

        final SemanticVersion streamProtocolVersion = blockNodeVersions.streamProtoVersion();
        assertNotNull(streamProtocolVersion);
        assertEquals(0, streamProtocolVersion.major());
        assertTrue(streamProtocolVersion.minor() > 70);

        final List<PluginVersion> pluginVersions = blockNodeVersions.installedPluginVersions();
        assertEquals(1, pluginVersions.size());
        final PluginVersion pluginVersion = pluginVersions.getFirst();
        assertEquals(plugin.getClass().getName(), pluginVersion.pluginId());
        assertEquals(semanticVersion, pluginVersion.pluginSoftwareVersion());
        // Features default to empty list
        assertEquals(0, pluginVersion.pluginFeatureNames().size());

        // Validate TssData defaults to null when not set
        assertNull(response.tssData());

        // Validate nodeAddressBook is absent when not loaded
        assertNull(response.nodeAddressBook());
        assertFalse(response.hasNodeAddressBook());
    }

    /**
     * Tests that the server status detail response changes when
     * {@link org.hiero.block.node.spi.BlockNodePlugin#onContextUpdate} is called,
     *
     * @throws ParseException if there is an error parsing the response
     */
    @Test
    @DisplayName("Should return changed Server Detail Status when the BlockNodeContext is updated")
    void shouldReturnValidServerStatusOnContextUpdate() throws ParseException {
        // notify the plugin of an update to the block node plugin
        BlockNodeContext newBlockNodeContext = new BlockNodeContext(
                blockNodeContext.configuration(),
                blockNodeContext.metricRegistry(),
                blockNodeContext.serverHealth(),
                blockNodeContext.blockMessaging(),
                blockNodeContext.historicalBlockProvider(),
                blockNodeContext.applicationStateFacility(),
                blockNodeContext.serviceLoader(),
                blockNodeContext.threadPoolManager(),
                BlockNodeVersions.DEFAULT,
                buildTssData(),
                null);
        plugin.onContextUpdate(newBlockNodeContext);

        ServerStatusRequest request = ServerStatusRequest.newBuilder().build();
        toPluginPipe.onNext(ServerStatusRequest.PROTOBUF.toBytes(request));
        assertEquals(1, fromPluginBytes.size());

        ServerStatusDetailResponse response = ServerStatusDetailResponse.PROTOBUF.parse(fromPluginBytes.getFirst());

        BlockNodeVersions blockNodeVersions = response.versionInformation();
        assertNotNull(blockNodeVersions);
        assertFalse(blockNodeVersions.hasStreamProtoVersion());
        assertFalse(blockNodeVersions.hasBlockNodeVersion());
        assertTrue(blockNodeVersions.installedPluginVersions().isEmpty());

        // Check the TssData
        TssData tssData = response.tssData();
        assertNotNull(tssData);
        assertEquals(Bytes.EMPTY, tssData.ledgerId());
        assertEquals(Bytes.EMPTY, tssData.wrapsVerificationKey());
        assertNotNull(tssData.currentRoster());

        // nodeAddressBook is absent because the context carried null
        assertNull(response.nodeAddressBook());
        assertFalse(response.hasNodeAddressBook());
    }

    @Test
    @DisplayName("Should include NodeAddressBook in response when context carries one")
    void shouldReturnNodeAddressBookWhenLoaded() throws ParseException {
        final NodeAddressBook book = buildAddressBook();
        final BlockNodeContext ctxWithBook = new BlockNodeContext(
                blockNodeContext.configuration(),
                blockNodeContext.metricRegistry(),
                blockNodeContext.serverHealth(),
                blockNodeContext.blockMessaging(),
                blockNodeContext.historicalBlockProvider(),
                blockNodeContext.applicationStateFacility(),
                blockNodeContext.serviceLoader(),
                blockNodeContext.threadPoolManager(),
                blockNodeContext.blockNodeVersions(),
                null,
                book);
        plugin.onContextUpdate(ctxWithBook);

        toPluginPipe.onNext(ServerStatusRequest.PROTOBUF.toBytes(
                ServerStatusRequest.newBuilder().build()));
        assertEquals(1, fromPluginBytes.size());

        final ServerStatusDetailResponse response =
                ServerStatusDetailResponse.PROTOBUF.parse(fromPluginBytes.getFirst());

        assertTrue(response.hasNodeAddressBook());
        final NodeAddressBook returned = response.nodeAddressBook();
        assertNotNull(returned);
        assertEquals(2, returned.nodeAddress().size());
        assertEquals(0L, returned.nodeAddress().get(0).nodeId());
        assertEquals("aabbcc", returned.nodeAddress().get(0).rsaPubKey());
        assertEquals(1L, returned.nodeAddress().get(1).nodeId());
        assertEquals("ddeeff", returned.nodeAddress().get(1).rsaPubKey());

        // TssData is absent because we passed null
        assertNull(response.tssData());
    }

    @Test
    @DisplayName("Should omit NodeAddressBook from response when context carries null")
    void shouldOmitNodeAddressBookWhenNotLoaded() throws ParseException {
        // Default context started with null nodeAddressBook — verify field absent in wire response
        toPluginPipe.onNext(ServerStatusRequest.PROTOBUF.toBytes(
                ServerStatusRequest.newBuilder().build()));
        assertEquals(1, fromPluginBytes.size());

        final ServerStatusDetailResponse response =
                ServerStatusDetailResponse.PROTOBUF.parse(fromPluginBytes.getFirst());

        assertFalse(response.hasNodeAddressBook());
        assertNull(response.nodeAddressBook());
    }

    NodeAddressBook buildAddressBook() {
        return NodeAddressBook.newBuilder()
                .nodeAddress(List.of(
                        NodeAddress.newBuilder().nodeId(0L).rsaPubKey("aabbcc").build(),
                        NodeAddress.newBuilder().nodeId(1L).rsaPubKey("ddeeff").build()))
                .build();
    }

    TssData buildTssData() {
        return TssData.newBuilder()
                .ledgerId(Bytes.EMPTY)
                .wrapsVerificationKey(Bytes.EMPTY)
                .currentRoster(TssRoster.DEFAULT)
                .build();
    }
}
