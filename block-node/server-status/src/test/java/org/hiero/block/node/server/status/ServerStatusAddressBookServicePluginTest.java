// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.server.status;

import static org.hiero.block.node.app.fixtures.TestUtils.enableDebugLogging;
import static org.hiero.block.node.base.ParseHelper.standardParse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.pbj.runtime.ParseException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import org.hiero.block.api.AddressBookRequest;
import org.hiero.block.api.AddressBookResponse;
import org.hiero.block.api.BlockNodeServiceInterface.BlockNodeServiceMethod;
import org.hiero.block.api.RangedAddressBookHistory;
import org.hiero.block.api.RangedNodeAddressBook;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.plugintest.GrpcPluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.spi.blockmessaging.AddressBookHistoryNotification;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Tests for the {@code getAddressBook} call of the {@link ServerStatusServicePlugin}. */
public class ServerStatusAddressBookServicePluginTest
        extends GrpcPluginTestBase<ServerStatusServicePlugin, BlockingExecutor, ScheduledExecutorService> {
    private final ServerStatusServicePlugin plugin = new ServerStatusServicePlugin();

    public ServerStatusAddressBookServicePluginTest() {
        super(
                new BlockingExecutor(new LinkedBlockingQueue<>()),
                new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        start(
                plugin,
                plugin.methods().stream()
                        .filter(method -> method.name().equals(BlockNodeServiceMethod.getAddressBook.name()))
                        .findFirst()
                        .orElseThrow(),
                new SimpleInMemoryHistoricalBlockFacility());
    }

    @BeforeEach
    void setup() {
        enableDebugLogging();
    }

    @Test
    @DisplayName("Returns an empty response when no address book is known yet")
    void returnsEmptyResponseWhenNoAddressBookIsKnown() throws ParseException {
        final AddressBookResponse response = getAddressBook(false);

        assertNull(response.nodeAddressBook());
        assertNull(response.rangedAddressBookHistory());
    }

    @Test
    @DisplayName("Returns the latest address book and the history by default")
    void returnsLatestAddressBookAndHistory() throws ParseException {
        blockMessaging.sendAddressBookHistoryUpdate(new AddressBookHistoryNotification(twoEraHistory()));

        final AddressBookResponse response = getAddressBook(false);

        assertNotNull(response.nodeAddressBook());
        assertEquals(1, response.nodeAddressBook().nodeAddress().size());
        assertEquals(
                "newer", response.nodeAddressBook().nodeAddress().getFirst().rsaPubKey());
        assertNotNull(response.rangedAddressBookHistory());
        assertEquals(2, response.rangedAddressBookHistory().addressBooks().size());
    }

    @Test
    @DisplayName("exclude_history returns only the latest address book")
    void excludeHistoryReturnsOnlyLatest() throws ParseException {
        blockMessaging.sendAddressBookHistoryUpdate(new AddressBookHistoryNotification(twoEraHistory()));

        final AddressBookResponse response = getAddressBook(true);

        assertNotNull(response.nodeAddressBook());
        assertEquals(
                "newer", response.nodeAddressBook().nodeAddress().getFirst().rsaPubKey());
        assertNull(response.rangedAddressBookHistory());
        assertFalse(response.hasRangedAddressBookHistory());
    }

    @Test
    @DisplayName("The history is in the order it was stored, oldest first")
    void historyKeepsItsOrder() throws ParseException {
        blockMessaging.sendAddressBookHistoryUpdate(new AddressBookHistoryNotification(twoEraHistory()));

        final List<RangedNodeAddressBook> eras =
                getAddressBook(false).rangedAddressBookHistory().addressBooks();

        assertEquals(0L, eras.getFirst().startBlock());
        assertEquals(
                "older", eras.getFirst().addressBook().nodeAddress().getFirst().rsaPubKey());
        assertTrue(eras.getLast().startBlock() > eras.getFirst().startBlock());
    }

    private AddressBookResponse getAddressBook(final boolean excludeHistory) throws ParseException {
        toPluginPipe.onNext(AddressBookRequest.PROTOBUF.toBytes(
                AddressBookRequest.newBuilder().excludeHistory(excludeHistory).build()));
        assertEquals(1, fromPluginBytes.size());
        return standardParse(AddressBookResponse.PROTOBUF, fromPluginBytes.getFirst());
    }

    private static RangedAddressBookHistory twoEraHistory() {
        return RangedAddressBookHistory.newBuilder()
                .addressBooks(List.of(era(0L, 99L, "older"), era(100L, -1L, "newer")))
                .build();
    }

    private static RangedNodeAddressBook era(final long start, final long end, final String key) {
        return RangedNodeAddressBook.newBuilder()
                .startBlock(start)
                .endBlock(end)
                .addressBook(NodeAddressBook.newBuilder()
                        .nodeAddress(List.of(NodeAddress.newBuilder()
                                .nodeId(0L)
                                .rsaPubKey(key)
                                .build()))
                        .build())
                .build();
    }
}
