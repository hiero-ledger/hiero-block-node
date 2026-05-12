// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.server;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.grpc.helidon.PbjRouting;
import com.hedera.pbj.grpc.helidon.PbjRouting.Builder;
import com.hedera.pbj.grpc.helidon.config.PbjConfig;
import com.hedera.pbj.runtime.Codec;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.common.socket.SocketOptions;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.WebServerConfig;
import java.util.ArrayList;
import java.util.List;
import org.hiero.block.api.BlockEnd;
import org.hiero.block.api.BlockItemSet;
import org.hiero.block.api.BlockNodeServiceInterface;
import org.hiero.block.api.BlockRange;
import org.hiero.block.api.BlockStreamSubscribeServiceInterface;
import org.hiero.block.api.RosterEntry;
import org.hiero.block.api.ServerStatusDetailResponse;
import org.hiero.block.api.ServerStatusRequest;
import org.hiero.block.api.ServerStatusResponse;
import org.hiero.block.api.SubscribeStreamRequest;
import org.hiero.block.api.SubscribeStreamResponse;
import org.hiero.block.api.SubscribeStreamResponse.Code;
import org.hiero.block.api.TssData;
import org.hiero.block.api.TssRoster;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.hiero.block.node.spi.historicalblocks.LongRange;

public class TestBlockNodeServer {
    private final WebServer webServer;

    public TestBlockNodeServer(int port, HistoricalBlockFacility historicalBlockFacility) {
        // Override the default message size in PBJ
        final PbjConfig pbjConfig = PbjConfig.builder()
                .name("pbj")
                .maxMessageSizeBytes(BlockAccessor.MAX_BLOCK_SIZE_BYTES)
                .build();

        // Create the service builder
        final Builder pbjRoutingBuilder = PbjRouting.builder()
                .service(new TrivialBlockNodeServerInterface(historicalBlockFacility))
                .service(new TestBlockStreamSubscribeService(historicalBlockFacility));
        // start the web server with the PBJ configuration and routing
        int BUFFER_SIZE_4_MB = 4 * 1024 * 1024;
        webServer = WebServerConfig.builder()
                .port(port)
                .addProtocol(pbjConfig)
                .addRouting(pbjRoutingBuilder)
                .connectionOptions(SocketOptions.builder()
                        .socketSendBufferSize(BUFFER_SIZE_4_MB)
                        .socketReceiveBufferSize(BUFFER_SIZE_4_MB)
                        .build())
                .writeBufferSize(BUFFER_SIZE_4_MB)
                .build();
        webServer.start();
    }

    /**
     * Returns the port the server is actually listening on.
     * Useful when the server was started with port 0 (OS-assigned ephemeral port).
     */
    public int port() {
        return webServer.port();
    }

    /**
     * Stop the web server.
     */
    public void stop() {
        if (webServer != null) {
            webServer.stop();
        }
    }

    /**
     * Test implementation of BlockStreamSubscribeService that serves blocks from the historical facility.
     * Simulates a real block node's streaming behavior for backfill integration tests.
     *
     * <p>Block items are streamed in batches of up to {@link #BATCH_SIZE_BYTES} to avoid exceeding
     * gRPC message size limits. Individual items larger than the batch threshold are sent alone.
     */
    private static final class TestBlockStreamSubscribeService implements BlockStreamSubscribeServiceInterface {
        private static final int BATCH_SIZE_BYTES = 2 * 1024 * 1024;

        private final HistoricalBlockFacility historicalBlockFacility;

        private TestBlockStreamSubscribeService(@NonNull final HistoricalBlockFacility historicalBlockFacility) {
            this.historicalBlockFacility = historicalBlockFacility;
        }

        @Override
        public void subscribeBlockStream(
                @NonNull final SubscribeStreamRequest request,
                @NonNull final Pipeline<? super SubscribeStreamResponse> replies) {
            boolean blocksAvailable = true;

            for (long i = request.startBlockNumber(); i <= request.endBlockNumber(); i++) {
                if (!historicalBlockFacility.availableBlocks().contains(i)) {
                    replies.onNext(SubscribeStreamResponse.newBuilder()
                            .status(SubscribeStreamResponse.Code.NOT_AVAILABLE)
                            .build());
                    blocksAvailable = false;
                    break;
                } else {
                    try (BlockAccessor accessor = historicalBlockFacility.block(i)) {
                        final Bytes blockBytes = accessor.blockBytes(BlockAccessor.Format.PROTOBUF);
                        Block block = Block.PROTOBUF.parse(
                                blockBytes.toReadableSequentialData(),
                                false,
                                false,
                                Codec.DEFAULT_MAX_DEPTH,
                                BlockAccessor.MAX_BLOCK_SIZE_BYTES);
                        sendBlockItemsInBatches(block.items(), replies);
                        replies.onNext(SubscribeStreamResponse.newBuilder()
                                .endOfBlock(BlockEnd.newBuilder().blockNumber(i).build())
                                .build());
                    } catch (ParseException e) {
                        replies.onNext(SubscribeStreamResponse.newBuilder()
                                .status(SubscribeStreamResponse.Code.NOT_AVAILABLE)
                                .build());
                        blocksAvailable = false;
                        break;
                    }
                }
            }

            if (blocksAvailable) {
                replies.onNext(SubscribeStreamResponse.newBuilder()
                        .status(Code.SUCCESS)
                        .build());
            }
            replies.onComplete();
        }

        /**
         * Sends block items in batches that stay within the gRPC message size limit.
         * Items larger than {@link #BATCH_SIZE_BYTES} are sent individually.
         */
        private static void sendBlockItemsInBatches(
                @NonNull final List<BlockItem> items,
                @NonNull final Pipeline<? super SubscribeStreamResponse> replies) {
            List<BlockItem> batch = new ArrayList<>();
            long batchSize = 0;

            for (BlockItem item : items) {
                long itemSize = estimateItemSize(item);

                if (!batch.isEmpty() && batchSize + itemSize > BATCH_SIZE_BYTES) {
                    sendBatch(batch, replies);
                    batch = new ArrayList<>();
                    batchSize = 0;
                }

                batch.add(item);
                batchSize += itemSize;

                if (itemSize > BATCH_SIZE_BYTES) {
                    sendBatch(batch, replies);
                    batch = new ArrayList<>();
                    batchSize = 0;
                }
            }

            if (!batch.isEmpty()) {
                sendBatch(batch, replies);
            }
        }

        private static void sendBatch(
                @NonNull final List<BlockItem> batch,
                @NonNull final Pipeline<? super SubscribeStreamResponse> replies) {
            replies.onNext(SubscribeStreamResponse.newBuilder()
                    .blockItems(BlockItemSet.newBuilder().blockItems(batch).build())
                    .build());
        }

        private static long estimateItemSize(@NonNull final BlockItem item) {
            return BlockItem.PROTOBUF.measureRecord(item);
        }
    }

    /**
     * Test implementation of BlockNodeService that returns server status from the historical facility.
     * Reports the available block range for backfill source discovery.
     */
    private static class TrivialBlockNodeServerInterface implements BlockNodeServiceInterface {
        private final HistoricalBlockFacility historicalBlockFacility;

        public TrivialBlockNodeServerInterface(final HistoricalBlockFacility historicalFacility) {
            historicalBlockFacility = historicalFacility;
        }

        @Override
        @NonNull
        public ServerStatusResponse serverStatus(@NonNull final ServerStatusRequest request) {
            // Returns the available block range from the historical facility
            return ServerStatusResponse.newBuilder()
                    .firstAvailableBlock(
                            historicalBlockFacility.availableBlocks().min())
                    .lastAvailableBlock(
                            historicalBlockFacility.availableBlocks().max())
                    .onlyLatestState(false)
                    .build();
        }

        @Override
        @NonNull
        public ServerStatusDetailResponse serverStatusDetail(@NonNull ServerStatusRequest request) {
            List<BlockRange> blockRanges = new ArrayList<>();

            BlockRange.Builder blockRangeBuilder = BlockRange.newBuilder();

            for (LongRange longRange :
                    historicalBlockFacility.availableBlocks().streamRanges().toList()) {
                blockRanges.add(blockRangeBuilder
                        .rangeStart(longRange.start())
                        .rangeEnd(longRange.end())
                        .build());
            }

            // Todo: add TssData flexibility to TestBlockNodeServer to allow tests to pass in TssData to hand back to
            // tests
            return ServerStatusDetailResponse.newBuilder()
                    .availableRanges(blockRangeBuilder.build())
                    .tssData(buildTssData(
                            Bytes.fromHex("01010101"),
                            Bytes.fromHex("02020202"),
                            List.of(buildRosterEntry(11, 22, Bytes.fromHex("03030303"))),
                            250,
                            50))
                    .build();
        }

        /// build a `TssData` object from individual fields from the `TssBootstrapConfig`
        ///
        /// @param ledgerId The ledgerId Bytes
        /// @param wrapsVerificationKey The wrapsVerificationKey Bytes
        /// @param validFromBlock The block from which this TssData is valid
        /// @param rosterValidFromBlock The block from which this TssRoster is valid
        /// @return a `TssData` object
        private TssData buildTssData(
                Bytes ledgerId,
                Bytes wrapsVerificationKey,
                List<RosterEntry> rosterEntries,
                long validFromBlock,
                long rosterValidFromBlock) {
            TssRoster tssRoster = TssRoster.newBuilder()
                    .rosterEntries(rosterEntries)
                    .validFromBlock(rosterValidFromBlock)
                    .build();
            return TssData.newBuilder()
                    .ledgerId(ledgerId)
                    .wrapsVerificationKey(wrapsVerificationKey)
                    .currentRoster(tssRoster)
                    .validFromBlock(validFromBlock)
                    .build();
        }

        /// build a `RosterEntry`object from individual fields
        ///
        /// @param nodeId The node id
        /// @param weight The weight
        /// @param schnorrPublicKey The schnorrPublicKey Bytes
        /// @return a `RosterEntry` object
        private RosterEntry buildRosterEntry(long nodeId, long weight, Bytes schnorrPublicKey) {
            return RosterEntry.newBuilder()
                    .nodeId(nodeId)
                    .weight(weight)
                    .schnorrPublicKey(schnorrPublicKey)
                    .build();
        }
    }
}
