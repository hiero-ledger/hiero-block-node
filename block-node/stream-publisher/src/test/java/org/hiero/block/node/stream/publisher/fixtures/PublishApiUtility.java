// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher.fixtures;

import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import org.hiero.block.api.BlockEnd;
import org.hiero.block.internal.BlockItemSetUnparsed;
import org.hiero.block.internal.PublishStreamRequestUnparsed;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.stream.publisher.PublisherHandler;

/// Utility class to assist with publish API testing.
public final class PublishApiUtility {

    /// Utility class, do not permit constructing instances.
    private PublishApiUtility() {}

    /// Send an `EndOfBlock` message for a given block number.
    /// @param publisherHandler A publisher handler to be tested.
    /// @param blockNumber A block number to close.
    public static void endThisBlock(final PublisherHandler publisherHandler, final long blockNumber) {
        final BlockEnd endOfBlock =
                BlockEnd.newBuilder().blockNumber(blockNumber).build();
        final PublishStreamRequestUnparsed request =
                PublishStreamRequestUnparsed.newBuilder().endOfBlock(endOfBlock).build();
        publisherHandler.onNext(request);
    }

    /// Send only the block header for a given block number, simulating a
    /// publisher that goes silent mid-block.
    /// @param publisherHandler A publisher handler to send the header to.
    /// @param blockNumber A block number whose header to send.
    public static void sendHeaderOnly(final PublisherHandler publisherHandler, final long blockNumber) {
        final BlockItemSetUnparsed headerOnly = BlockItemSetUnparsed.newBuilder()
                .blockItems(List.of(
                        TestBlockBuilder.generateBlockWithNumber(blockNumber).getHeaderUnparsed()))
                .build();
        final PublishStreamRequestUnparsed request =
                PublishStreamRequestUnparsed.newBuilder().blockItems(headerOnly).build();
        publisherHandler.onNext(request);
    }

    /// Send an `EndOfBlock` message for a given block number.
    /// @param pluginPipe A publisher plugin pipeline to send requests.
    /// @param blockNumber A block number to close.
    public static void endThisBlock(final Pipeline<? super Bytes> pluginPipe, final long blockNumber) {
        final BlockEnd endOfBlock =
                BlockEnd.newBuilder().blockNumber(blockNumber).build();
        final PublishStreamRequestUnparsed request =
                PublishStreamRequestUnparsed.newBuilder().endOfBlock(endOfBlock).build();
        pluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(request));
    }
}
