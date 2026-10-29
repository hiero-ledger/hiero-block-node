// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder.generateBlockWithNumber;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.hedera.pbj.runtime.grpc.GrpcCall;
import com.hedera.pbj.runtime.grpc.GrpcClient;
import com.hedera.pbj.runtime.grpc.Pipeline;
import java.util.List;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import org.hiero.block.api.BlockEnd;
import org.hiero.block.api.SubscribeStreamResponse;
import org.hiero.block.internal.BlockItemSetUnparsed;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.internal.SubscribeStreamResponseUnparsed;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class BlockStreamSubscribeUnparsedClientTest {

    @Test
    @DisplayName("getBatchOfBlocks times out and fails when no response is ever received")
    void awaitTimesOutWhenNoResponseIsReceived() {
        final GrpcClient grpcClient = mock(GrpcClient.class);
        final GrpcCall<?, ?> grpcCall = mock(GrpcCall.class);
        // Never invoke the pipeline callbacks, so RequestContext.await() has nothing to wake it up
        // other than the timeout itself.
        doReturn(grpcCall).when(grpcClient).createCall(any(), any(), any(), any(), anyMap());

        final BlockStreamSubscribeUnparsedClient client = new BlockStreamSubscribeUnparsedClient(grpcClient, 50L);

        assertThatThrownBy(() -> client.getBatchOfBlocks(1, 2))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Error fetching blocks")
                .cause()
                .hasMessage("Timed out waiting for block stream response");
    }

    /// Builds a client whose gRPC call plays the scripted peer behavior into the request's pipeline as
    /// soon as the request is sent.
    private static BlockStreamSubscribeUnparsedClient clientWithPeer(
            final Consumer<Pipeline<SubscribeStreamResponseUnparsed>> peer) {
        final GrpcClient grpcClient = mock(GrpcClient.class);
        doAnswer(invocation -> {
                    final Pipeline<SubscribeStreamResponseUnparsed> pipeline = invocation.getArgument(3);
                    final GrpcCall<?, ?> call = mock(GrpcCall.class);
                    doAnswer(sent -> {
                                peer.accept(pipeline);
                                return null;
                            })
                            .when(call)
                            .sendRequest(any(), anyBoolean());
                    return call;
                })
                .when(grpcClient)
                .createCall(any(), any(), any(), any(), anyMap());
        return new BlockStreamSubscribeUnparsedClient(grpcClient, 5_000L);
    }

    private static SubscribeStreamResponseUnparsed items(final List<BlockItemUnparsed> blockItems) {
        return SubscribeStreamResponseUnparsed.newBuilder()
                .blockItems(
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build())
                .build();
    }

    private static SubscribeStreamResponseUnparsed endOfBlock(final long blockNumber) {
        return SubscribeStreamResponseUnparsed.newBuilder()
                .endOfBlock(BlockEnd.newBuilder().blockNumber(blockNumber).build())
                .build();
    }

    private static void sendFullBlock(final Pipeline<SubscribeStreamResponseUnparsed> pipeline, final long number) {
        final BlockUnparsed block = generateBlockWithNumber(number).blockUnparsed();
        pipeline.onNext(items(block.blockItems()));
        pipeline.onNext(endOfBlock(number));
    }

    @Nested
    @DisplayName("Bounded block buffering")
    class BoundedBufferingTests {

        @Test
        @DisplayName("Returns every requested block when the peer sends exactly the requested range")
        void returnsRequestedBlocks() {
            final BlockStreamSubscribeUnparsedClient client = clientWithPeer(pipeline -> {
                sendFullBlock(pipeline, 3);
                sendFullBlock(pipeline, 4);
                pipeline.onNext(SubscribeStreamResponseUnparsed.newBuilder()
                        .status(SubscribeStreamResponse.Code.SUCCESS)
                        .build());
                pipeline.onComplete();
            });

            final List<BlockUnparsed> blocks = client.getBatchOfBlocks(3, 4);

            assertThat(blocks).hasSize(2);
            assertThat(blocks.getFirst()).isEqualTo(generateBlockWithNumber(3).blockUnparsed());
            assertThat(blocks.getLast()).isEqualTo(generateBlockWithNumber(4).blockUnparsed());
        }

        @Test
        @DisplayName("Stops at the requested count and ignores blocks the peer sends beyond the range")
        void stopsAtRequestedCount() {
            final BlockStreamSubscribeUnparsedClient client = clientWithPeer(pipeline -> {
                sendFullBlock(pipeline, 3);
                sendFullBlock(pipeline, 4);
                // Anything after the last requested block must be dropped, not buffered or validated.
                sendFullBlock(pipeline, 5);
                sendFullBlock(pipeline, 6);
                pipeline.onError(new RuntimeException("late failure"));
            });

            final List<BlockUnparsed> blocks = client.getBatchOfBlocks(3, 4);

            assertThat(blocks).hasSize(2);
        }

        @Test
        @DisplayName("A failure reported after the requested blocks arrived does not fail the request")
        void lateErrorAfterCompletionIsIgnored() {
            final BlockStreamSubscribeUnparsedClient client = clientWithPeer(pipeline -> {
                sendFullBlock(pipeline, 7);
                pipeline.onError(new RuntimeException("connection reset"));
            });

            assertThat(client.getBatchOfBlocks(7, 7)).hasSize(1);
        }

        @Test
        @DisplayName("A failure is not overwritten by blocks that arrive after it")
        void failureIsNotOverwrittenByLaterCompletion() {
            final BlockStreamSubscribeUnparsedClient client = clientWithPeer(pipeline -> {
                sendFullBlock(pipeline, 7);
                pipeline.onError(new RuntimeException("connection reset"));
                sendFullBlock(pipeline, 8);
            });

            assertThatThrownBy(() -> client.getBatchOfBlocks(7, 8))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessage("Error fetching blocks")
                    .cause()
                    .hasMessage("connection reset");
        }

        @Test
        @DisplayName(
                "A timeout racing with the last block ends as either the full range or a failure, never a partial list")
        void timeoutRacingWithCompletionNeverReturnsPartialBlocks() {
            for (int attempt = 0; attempt < 100; attempt++) {
                final GrpcClient grpcClient = mock(GrpcClient.class);
                doAnswer(invocation -> {
                            final Pipeline<SubscribeStreamResponseUnparsed> pipeline = invocation.getArgument(3);
                            final GrpcCall<?, ?> call = mock(GrpcCall.class);
                            doAnswer(sent -> {
                                        sendFullBlock(pipeline, 7);
                                        Thread.ofPlatform().daemon().start(() -> {
                                            LockSupport.parkNanos(1_000_000L);
                                            sendFullBlock(pipeline, 8);
                                        });
                                        return null;
                                    })
                                    .when(call)
                                    .sendRequest(any(), anyBoolean());
                            return call;
                        })
                        .when(grpcClient)
                        .createCall(any(), any(), any(), any(), anyMap());
                final BlockStreamSubscribeUnparsedClient client =
                        new BlockStreamSubscribeUnparsedClient(grpcClient, 1L);

                try {
                    assertThat(client.getBatchOfBlocks(7, 8)).hasSize(2);
                } catch (final RuntimeException timedOut) {
                    assertThat(timedOut).hasMessage("Error fetching blocks");
                }
            }
        }

        @Test
        @DisplayName("Fails when a block header is above the requested range")
        void failsForHeaderAboveRange() {
            final BlockStreamSubscribeUnparsedClient client = clientWithPeer(pipeline -> {
                sendFullBlock(pipeline, 3);
                sendFullBlock(pipeline, 9);
            });

            assertThatThrownBy(() -> client.getBatchOfBlocks(3, 5))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessage("Error fetching blocks")
                    .cause()
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("outside the requested range 3 to 5");
        }

        @Test
        @DisplayName("Fails when a block header is below the requested range")
        void failsForHeaderBelowRange() {
            final BlockStreamSubscribeUnparsedClient client = clientWithPeer(pipeline -> sendFullBlock(pipeline, 1));

            assertThatThrownBy(() -> client.getBatchOfBlocks(3, 4))
                    .isInstanceOf(RuntimeException.class)
                    .cause()
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("outside the requested range 3 to 4");
        }

        @Test
        @DisplayName("Fails when a header skips an in-range block")
        void failsForOutOfOrderHeader() {
            final BlockStreamSubscribeUnparsedClient client = clientWithPeer(pipeline -> {
                sendFullBlock(pipeline, 3);
                sendFullBlock(pipeline, 5);
            });

            assertThatThrownBy(() -> client.getBatchOfBlocks(3, 6))
                    .isInstanceOf(RuntimeException.class)
                    .cause()
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Expected block number 4 but received 5");
        }

        @Test
        @DisplayName("Fails when block items arrive before any block header")
        void failsForItemsBeforeHeader() {
            final List<BlockItemUnparsed> allItems =
                    generateBlockWithNumber(3).blockUnparsed().blockItems();
            final List<BlockItemUnparsed> withoutHeader = allItems.subList(1, allItems.size());
            final BlockStreamSubscribeUnparsedClient client =
                    clientWithPeer(pipeline -> pipeline.onNext(items(withoutHeader)));

            assertThatThrownBy(() -> client.getBatchOfBlocks(3, 4))
                    .isInstanceOf(RuntimeException.class)
                    .cause()
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("before a block header");
        }

        @Test
        @DisplayName("Fails when an end of block arrives without a block header")
        void failsForEndOfBlockWithoutHeader() {
            final BlockStreamSubscribeUnparsedClient client =
                    clientWithPeer(pipeline -> pipeline.onNext(endOfBlock(3)));

            assertThatThrownBy(() -> client.getBatchOfBlocks(3, 4))
                    .isInstanceOf(RuntimeException.class)
                    .cause()
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("end of block without a block header");
        }
    }
}
