// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.server.status;

import static org.hiero.block.node.app.fixtures.TestUtils.enableDebugLogging;
import static org.hiero.block.node.base.ParseHelper.standardParse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import org.hiero.block.api.BlockNodeServiceInterface.BlockNodeServiceMethod;
import org.hiero.block.api.TssData;
import org.hiero.block.api.TssDataRequest;
import org.hiero.block.api.TssDataResponse;
import org.hiero.block.api.TssRoster;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.plugintest.GrpcPluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.spi.blockmessaging.TssDataNotification;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Tests for the {@code getTssData} call of the {@link ServerStatusServicePlugin}. */
public class ServerStatusTssDataServicePluginTest
        extends GrpcPluginTestBase<ServerStatusServicePlugin, BlockingExecutor, ScheduledExecutorService> {
    private final ServerStatusServicePlugin plugin = new ServerStatusServicePlugin();

    public ServerStatusTssDataServicePluginTest() {
        super(
                new BlockingExecutor(new LinkedBlockingQueue<>()),
                new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        start(
                plugin,
                plugin.methods().stream()
                        .filter(method -> method.name().equals(BlockNodeServiceMethod.getTssData.name()))
                        .findFirst()
                        .orElseThrow(),
                new SimpleInMemoryHistoricalBlockFacility());
    }

    @BeforeEach
    void setup() {
        enableDebugLogging();
    }

    @Test
    @DisplayName("Returns an empty response when no TSS data is known yet")
    void returnsEmptyResponseWhenNoTssDataIsKnown() throws ParseException {
        final TssDataResponse response = getTssData(false);

        assertNull(response.tssData());
        assertNull(response.rangedTssDataHistory());
    }

    @Test
    @DisplayName("Returns the latest TSS data")
    void returnsLatestTssData() throws ParseException {
        blockMessaging.sendTssDataUpdate(new TssDataNotification(sampleTssData()));

        final TssDataResponse response = getTssData(true);

        assertNotNull(response.tssData());
        assertEquals(Bytes.fromHex("0a0b"), response.tssData().ledgerId());
        assertEquals(42L, response.tssData().validFromBlock());
    }

    @Test
    @DisplayName("The history is not filled in yet, even when it is requested")
    void historyIsNotFilledInYet() throws ParseException {
        blockMessaging.sendTssDataUpdate(new TssDataNotification(sampleTssData()));

        final TssDataResponse response = getTssData(false);

        assertNotNull(response.tssData());
        assertNull(response.rangedTssDataHistory());
    }

    private TssDataResponse getTssData(final boolean excludeHistory) throws ParseException {
        toPluginPipe.onNext(TssDataRequest.PROTOBUF.toBytes(
                TssDataRequest.newBuilder().excludeHistory(excludeHistory).build()));
        assertEquals(1, fromPluginBytes.size());
        return standardParse(TssDataResponse.PROTOBUF, fromPluginBytes.getFirst());
    }

    private static TssData sampleTssData() {
        return TssData.newBuilder()
                .ledgerId(Bytes.fromHex("0a0b"))
                .wrapsVerificationKey(Bytes.EMPTY)
                .currentRoster(TssRoster.DEFAULT)
                .validFromBlock(42L)
                .build();
    }
}
