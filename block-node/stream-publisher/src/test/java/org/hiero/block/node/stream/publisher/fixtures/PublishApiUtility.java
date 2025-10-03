// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher.fixtures;

import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import org.hiero.block.api.BlockEnd;
import org.hiero.block.internal.PublishStreamRequestUnparsed;
import org.hiero.block.node.stream.publisher.PublisherHandler;

/**
 * Utility class to assist with publish API testing.
 */
public final class PublishApiUtility {

    /**
     * Utility class, do not permit constructing instances.
     */
    private PublishApiUtility() {}

    /**
     * Send an `EndOfBlock` message for a given block number.
     * @param publisherHandler A publisher handler to be tested.
     * @param blockNumber A block number to close.
     */
    public static void endThisBlock(final PublisherHandler publisherHandler, final long blockNumber) {
        final BlockEnd endOfBlock =
                BlockEnd.newBuilder().blockNumber(blockNumber).build();
        final PublishStreamRequestUnparsed request =
                PublishStreamRequestUnparsed.newBuilder().endOfBlock(endOfBlock).build();
        publisherHandler.onNext(request);
    }

    /**
     * Send an `EndOfBlock` message for a given block number.
     * @param pluginPipe A publisher plugin pipeline to send requests.
     * @param blockNumber A block number to close.
     */
    public static void endThisBlock(final Pipeline<? super Bytes> pluginPipe, final long blockNumber) {
        final BlockEnd endOfBlock =
                BlockEnd.newBuilder().blockNumber(blockNumber).build();
        final PublishStreamRequestUnparsed request =
                PublishStreamRequestUnparsed.newBuilder().endOfBlock(endOfBlock).build();
        pluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(request));
    }
}
