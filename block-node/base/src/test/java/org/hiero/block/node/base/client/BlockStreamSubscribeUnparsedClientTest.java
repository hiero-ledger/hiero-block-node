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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import com.hedera.pbj.runtime.grpc.GrpcCall;
import com.hedera.pbj.runtime.grpc.GrpcClient;
import com.hedera.pbj.runtime.grpc.Pipeline;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import org.hiero.block.api.BlockEnd;
import org.hiero.block.api.SubscribeStreamResponse;
import org.hiero.block.internal.BlockItemSetUnparsed;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.internal.SubscribeStreamResponseUnparsed;
import org.hiero.block.node.base.client.BlockStreamSubscribeUnparsedClient.BatchResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class BlockStreamSubscribeUnparsedClientTest {

    @Test
    @DisplayName("getBatchOfBlocks times out and returns the error when no response is ever received")
    void awaitTimesOutWhenNoResponseIsReceived() {
        final GrpcClient grpcClient = mock(GrpcClient.class);
        final GrpcCall<?, ?> grpcCall = mock(GrpcCall.class);
        // Never invoke the pipeline callbacks, so RequestContext.await() has nothing to wake it up
        // other than the timeout itself.
        doReturn(grpcCall).when(grpcClient).createCall(any(), any(), any(), any(), anyMap());

        final BlockStreamSubscribeUnparsedClient client =
                new BlockStreamSubscribeUnparsedClient(grpcClient, 5_000L, 50L);

        final BatchResult result = client.getBatchOfBlocks(1, 2);

        assertThat(result.blocks()).isEmpty();
        assertThat(result.error())
                .hasMessage(
                        "No block stream response received within the inactivity timeout (BACKFILL_GRPC_INACTIVITY_TIMEOUT=50 ms)");
    }

    @Test
    @DisplayName("getBatchOfBlocks rejects an invalid range")
    void rejectsInvalidRange() {
        final BlockStreamSubscribeUnparsedClient client = clientWithPeer(pipeline -> {});

        assertThatThrownBy(() -> client.getBatchOfBlocks(2, 1)).isInstanceOf(IllegalArgumentException.class);
    }

    /// Builds a client whose gRPC call plays the scripted peer behavior into the request's pipeline as
    /// soon as the request is sent.
    private static BlockStreamSubscribeUnparsedClient clientWithPeer(
            final Consumer<Pipeline<SubscribeStreamResponseUnparsed>> peer) {
        return clientWithPeer(peer, 5_000L, 5_000L);
    }

    /// Same as [#clientWithPeer(Consumer)], with the given request timeouts.
    private static BlockStreamSubscribeUnparsedClient clientWithPeer(
            final Consumer<Pipeline<SubscribeStreamResponseUnparsed>> peer,
            final long overallTimeoutMs,
            final long inactivityTimeoutMs) {
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
        return new BlockStreamSubscribeUnparsedClient(grpcClient, overallTimeoutMs, inactivityTimeoutMs);
    }

    /// Sends blocks `first` to `last` from a background thread, one every `intervalMs`, then runs `atEnd`.
    /// The thread stops early, without running `atEnd`, once `stop` is set.
    /// The blocks are built up front, before the request starts, so the time to build them does not add to
    /// the gaps the request's timeouts measure.
    private static Consumer<Pipeline<SubscribeStreamResponseUnparsed>> slowPeer(
            final long first,
            final long last,
            final long intervalMs,
            final AtomicBoolean stop,
            final Consumer<Pipeline<SubscribeStreamResponseUnparsed>> atEnd) {
        final List<BlockUnparsed> blocks = new ArrayList<>();
        for (long number = first; number <= last; number++) {
            blocks.add(generateBlockWithNumber(number).blockUnparsed());
        }
        return pipeline -> Thread.ofPlatform().daemon().start(() -> {
            for (int i = 0; i < blocks.size() && !stop.get(); i++) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(intervalMs));
                pipeline.onNext(items(blocks.get(i).blockItems()));
                pipeline.onNext(endOfBlock(first + i));
            }
            if (!stop.get()) {
                atEnd.accept(pipeline);
            }
        });
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

            final BatchResult result = client.getBatchOfBlocks(3, 4);

            assertThat(result.error()).isNull();
            assertThat(result.blocks()).hasSize(2);
            assertThat(result.blocks().getFirst())
                    .isEqualTo(generateBlockWithNumber(3).blockUnparsed());
            assertThat(result.blocks().getLast())
                    .isEqualTo(generateBlockWithNumber(4).blockUnparsed());
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

            final BatchResult result = client.getBatchOfBlocks(3, 4);

            assertThat(result.error()).isNull();
            assertThat(result.blocks()).hasSize(2);
        }

        @Test
        @DisplayName("A failure reported after the requested blocks arrived does not fail the request")
        void lateErrorAfterCompletionIsIgnored() {
            final BlockStreamSubscribeUnparsedClient client = clientWithPeer(pipeline -> {
                sendFullBlock(pipeline, 7);
                pipeline.onError(new RuntimeException("connection reset"));
            });

            final BatchResult result = client.getBatchOfBlocks(7, 7);

            assertThat(result.error()).isNull();
            assertThat(result.blocks()).hasSize(1);
        }

        @Test
        @DisplayName("A failure is not overwritten by blocks that arrive after it")
        void failureIsNotOverwrittenByLaterCompletion() {
            final BlockStreamSubscribeUnparsedClient client = clientWithPeer(pipeline -> {
                sendFullBlock(pipeline, 7);
                pipeline.onError(new RuntimeException("connection reset"));
                sendFullBlock(pipeline, 8);
            });

            final BatchResult result = client.getBatchOfBlocks(7, 8);

            assertThat(result.error()).hasMessage("connection reset");
            assertThat(result.blocks())
                    .containsExactly(generateBlockWithNumber(7).blockUnparsed());
        }

        @Test
        @DisplayName("A timeout racing with the last block ends as either the full range or a failure with a prefix")
        void timeoutRacingWithCompletionReturnsContiguousPrefix() {
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
                        new BlockStreamSubscribeUnparsedClient(grpcClient, 1L, 1L);

                final BatchResult result = client.getBatchOfBlocks(7, 8);

                assertThat(result.blocks().getFirst())
                        .isEqualTo(generateBlockWithNumber(7).blockUnparsed());
                if (result.error() == null) {
                    assertThat(result.blocks()).hasSize(2);
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

            final BatchResult result = client.getBatchOfBlocks(3, 5);

            assertThat(result.blocks())
                    .containsExactly(generateBlockWithNumber(3).blockUnparsed());
            assertThat(result.error())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("outside the requested range 3 to 5");
        }

        @Test
        @DisplayName("Fails when a block header is below the requested range")
        void failsForHeaderBelowRange() {
            final BlockStreamSubscribeUnparsedClient client = clientWithPeer(pipeline -> sendFullBlock(pipeline, 1));

            final BatchResult result = client.getBatchOfBlocks(3, 4);

            assertThat(result.blocks()).isEmpty();
            assertThat(result.error())
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

            final BatchResult result = client.getBatchOfBlocks(3, 6);

            assertThat(result.blocks())
                    .containsExactly(generateBlockWithNumber(3).blockUnparsed());
            assertThat(result.error())
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

            final BatchResult result = client.getBatchOfBlocks(3, 4);

            assertThat(result.blocks()).isEmpty();
            assertThat(result.error())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("before a block header");
        }

        @Test
        @DisplayName("Fails when an end of block arrives without a block header")
        void failsForEndOfBlockWithoutHeader() {
            final BlockStreamSubscribeUnparsedClient client =
                    clientWithPeer(pipeline -> pipeline.onNext(endOfBlock(3)));

            final BatchResult result = client.getBatchOfBlocks(3, 4);

            assertThat(result.blocks()).isEmpty();
            assertThat(result.error())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("end of block without a block header");
        }
    }

    @Nested
    @DisplayName("Partial batches")
    class PartialBatchTests {

        @Test
        @DisplayName("Returns the blocks received before a stream error, together with the error")
        void returnsPrefixAndErrorOnStreamError() {
            final BlockStreamSubscribeUnparsedClient client = clientWithPeer(pipeline -> {
                sendFullBlock(pipeline, 3);
                sendFullBlock(pipeline, 4);
                sendFullBlock(pipeline, 5);
                pipeline.onError(new RuntimeException("connection reset"));
            });

            final BatchResult result = client.getBatchOfBlocks(3, 7);

            assertThat(result.blocks()).hasSize(3);
            assertThat(result.error()).hasMessage("connection reset");
        }

        @Test
        @DisplayName("Returns the blocks received before an error status")
        void returnsPrefixOnErrorStatus() {
            final BlockStreamSubscribeUnparsedClient client = clientWithPeer(pipeline -> {
                sendFullBlock(pipeline, 3);
                pipeline.onNext(SubscribeStreamResponseUnparsed.newBuilder()
                        .status(SubscribeStreamResponse.Code.NOT_AVAILABLE)
                        .build());
            });

            final BatchResult result = client.getBatchOfBlocks(3, 4);

            assertThat(result.blocks()).hasSize(1);
            assertThat(result.error()).hasMessageContaining("NOT_AVAILABLE");
        }

        @Test
        @DisplayName("A timeout keeps complete blocks and drops the block without an end of block")
        void timeoutDropsPartialBlock() {
            final GrpcClient grpcClient = mock(GrpcClient.class);
            doAnswer(invocation -> {
                        final Pipeline<SubscribeStreamResponseUnparsed> pipeline = invocation.getArgument(3);
                        final GrpcCall<?, ?> call = mock(GrpcCall.class);
                        doAnswer(sent -> {
                                    sendFullBlock(pipeline, 3);
                                    sendFullBlock(pipeline, 4);
                                    pipeline.onNext(items(generateBlockWithNumber(5)
                                            .blockUnparsed()
                                            .blockItems()));
                                    return null;
                                })
                                .when(call)
                                .sendRequest(any(), anyBoolean());
                        return call;
                    })
                    .when(grpcClient)
                    .createCall(any(), any(), any(), any(), anyMap());
            final BlockStreamSubscribeUnparsedClient client =
                    new BlockStreamSubscribeUnparsedClient(grpcClient, 50L, 5_000L);

            final BatchResult result = client.getBatchOfBlocks(3, 6);

            assertThat(result.blocks()).hasSize(2);
            assertThat(result.error())
                    .hasMessage("Timed out waiting for block stream response (BACKFILL_GRPC_OVERALL_TIMEOUT=50 ms)");
        }

        @Test
        @DisplayName("A stream that ends with a success status after fewer blocks has no error")
        void shortStreamWithSuccessStatusHasNoError() {
            final BlockStreamSubscribeUnparsedClient client = clientWithPeer(pipeline -> {
                sendFullBlock(pipeline, 3);
                sendFullBlock(pipeline, 4);
                pipeline.onNext(SubscribeStreamResponseUnparsed.newBuilder()
                        .status(SubscribeStreamResponse.Code.SUCCESS)
                        .build());
                pipeline.onComplete();
            });

            final BatchResult result = client.getBatchOfBlocks(3, 7);

            assertThat(result.blocks()).hasSize(2);
            assertThat(result.error()).isNull();
        }

        @Test
        @DisplayName("Interruption returns the blocks received with an InterruptedException and keeps the flag")
        void interruptionReturnsPrefix() {
            final BlockStreamSubscribeUnparsedClient client = clientWithPeer(pipeline -> {
                sendFullBlock(pipeline, 3);
                Thread.currentThread().interrupt();
            });

            final BatchResult result = client.getBatchOfBlocks(3, 4);

            // Thread.interrupted() also clears the flag, so it cannot leak into later tests.
            assertThat(Thread.interrupted()).isTrue();
            assertThat(result.blocks()).hasSize(1);
            assertThat(result.error()).isInstanceOf(InterruptedException.class);
        }

        @Test
        @DisplayName("Blocks that arrive after a timeout are not added to the request")
        void blocksAfterTimeoutAreNotAdded() {
            final AtomicReference<Pipeline<SubscribeStreamResponseUnparsed>> captured = new AtomicReference<>();
            final GrpcClient grpcClient = mock(GrpcClient.class);
            doAnswer(invocation -> {
                        final Pipeline<SubscribeStreamResponseUnparsed> pipeline = invocation.getArgument(3);
                        captured.set(pipeline);
                        final GrpcCall<?, ?> call = mock(GrpcCall.class);
                        doAnswer(sent -> {
                                    sendFullBlock(pipeline, 3);
                                    return null;
                                })
                                .when(call)
                                .sendRequest(any(), anyBoolean());
                        return call;
                    })
                    .when(grpcClient)
                    .createCall(any(), any(), any(), any(), anyMap());
            final BlockStreamSubscribeUnparsedClient client =
                    new BlockStreamSubscribeUnparsedClient(grpcClient, 50L, 5_000L);

            final BatchResult result = client.getBatchOfBlocks(3, 5);
            // The abandoned stream keeps delivering, including the rest of the range and a late error.
            sendFullBlock(captured.get(), 4);
            sendFullBlock(captured.get(), 5);
            captured.get().onError(new RuntimeException("connection closed"));

            assertThat(result.blocks())
                    .containsExactly(generateBlockWithNumber(3).blockUnparsed());
            assertThat(result.error())
                    .hasMessage("Timed out waiting for block stream response (BACKFILL_GRPC_OVERALL_TIMEOUT=50 ms)");
        }

        @Test
        @DisplayName("A failure to create the call returns no blocks and the error instead of throwing")
        void failureToCreateCallReturnsError() {
            final GrpcClient grpcClient = mock(GrpcClient.class);
            final RuntimeException failure = new IllegalStateException("connection is closed");
            doThrow(failure).when(grpcClient).createCall(any(), any(), any(), any(), anyMap());
            final BlockStreamSubscribeUnparsedClient client =
                    new BlockStreamSubscribeUnparsedClient(grpcClient, 5_000L, 5_000L);

            final BatchResult result = client.getBatchOfBlocks(3, 4);

            assertThat(result.blocks()).isEmpty();
            assertThat(result.error()).isSameAs(failure);
        }

        @Test
        @DisplayName("A failure to send the request returns no blocks and the error instead of throwing")
        void failureToSendRequestReturnsError() {
            final GrpcClient grpcClient = mock(GrpcClient.class);
            final GrpcCall<?, ?> call = mock(GrpcCall.class);
            final RuntimeException failure = new IllegalStateException("stream reset");
            doThrow(failure).when(call).sendRequest(any(), anyBoolean());
            doReturn(call).when(grpcClient).createCall(any(), any(), any(), any(), anyMap());
            final BlockStreamSubscribeUnparsedClient client =
                    new BlockStreamSubscribeUnparsedClient(grpcClient, 5_000L, 5_000L);

            final BatchResult result = client.getBatchOfBlocks(3, 4);

            assertThat(result.blocks()).isEmpty();
            assertThat(result.error()).isSameAs(failure);
        }
    }

    @Nested
    @DisplayName("Progress-based timeout")
    class ProgressTimeoutTests {

        @Test
        @Timeout(10)
        @DisplayName("A stream that keeps sending completes even when it takes longer than the inactivity timeout")
        void steadyStreamOutlivesInactivityTimeout() {
            // 20 blocks, 20 ms apart, take about 400 ms in total: longer than the 300 ms inactivity timeout,
            // while each gap stays far below it.
            final BlockStreamSubscribeUnparsedClient client = clientWithPeer(
                    slowPeer(3, 22, 20, new AtomicBoolean(), pipeline -> {
                        pipeline.onNext(SubscribeStreamResponseUnparsed.newBuilder()
                                .status(SubscribeStreamResponse.Code.SUCCESS)
                                .build());
                        pipeline.onComplete();
                    }),
                    5_000L,
                    300L);

            final BatchResult result = client.getBatchOfBlocks(3, 22);

            assertThat(result.error()).isNull();
            assertThat(result.blocks()).hasSize(20);
        }

        @Test
        @Timeout(10)
        @DisplayName("A stream that stops sending ends after the inactivity timeout with the blocks received")
        void stalledStreamEndsAfterInactivityTimeout() {
            final BlockStreamSubscribeUnparsedClient client = clientWithPeer(
                    pipeline -> {
                        sendFullBlock(pipeline, 3);
                        sendFullBlock(pipeline, 4);
                    },
                    5_000L,
                    200L);

            final long startNanos = System.nanoTime();
            final BatchResult result = client.getBatchOfBlocks(3, 6);
            final long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

            assertThat(result.blocks()).hasSize(2);
            assertThat(result.error())
                    .hasMessage(
                            "No block stream response received within the inactivity timeout (BACKFILL_GRPC_INACTIVITY_TIMEOUT=200 ms)");
            assertThat(elapsedMs).isBetween(200L, 2_000L);
        }

        @Test
        @Timeout(10)
        @DisplayName("A stream that keeps trickling ends at the overall timeout with the blocks received")
        void tricklingStreamEndsAtOverallTimeout() {
            // A block every 20 ms never trips the 1 s inactivity timeout, and 100 blocks would take 2 s.
            final AtomicBoolean stopPeer = new AtomicBoolean();
            final BlockStreamSubscribeUnparsedClient client =
                    clientWithPeer(slowPeer(3, 102, 20, stopPeer, pipeline -> {}), 1_500L, 1_000L);

            final long startNanos = System.nanoTime();
            final BatchResult result = client.getBatchOfBlocks(3, 102);
            final long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            // The request is finished, so the peer has nothing left to send to.
            stopPeer.set(true);

            assertThat(result.error())
                    .hasMessage("Timed out waiting for block stream response (BACKFILL_GRPC_OVERALL_TIMEOUT=1500 ms)");
            assertThat(result.blocks()).isNotEmpty().hasSizeLessThan(100);
            assertThat(result.blocks().getFirst())
                    .isEqualTo(generateBlockWithNumber(3).blockUnparsed());
            assertThat(elapsedMs).isBetween(1_500L, 4_000L);
        }
    }
}
