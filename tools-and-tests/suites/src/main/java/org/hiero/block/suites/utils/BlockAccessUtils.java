// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.utils;

import static java.util.Objects.requireNonNull;

import edu.umd.cs.findbugs.annotations.NonNull;
import org.hiero.block.api.protoc.BlockAccessServiceGrpc;
import org.hiero.block.api.protoc.BlockRequest;
import org.hiero.block.api.protoc.BlockRequest.Builder;
import org.hiero.block.api.protoc.BlockResponse;

/**
 * Utility class for block access operations.
 * Contains methods for creating block requests and interacting with the Block Node gRPC service.
 */
public final class BlockAccessUtils {

    private BlockAccessUtils() {
        // Prevent instantiation
    }

    /**
     * Creates a SingleBlockRequest to retrieve a specific block.
     *
     * @param blockNumber The block number to retrieve
     * @param latest Whether to retrieve the latest block
     * @return A SingleBlockRequest object
     */
    public static BlockRequest createGetBlockRequest(long blockNumber, boolean latest) {
        final Builder builder = BlockRequest.newBuilder().setBlockNumber(blockNumber);
        if (latest) {
            builder.setRetrieveLatest(true);
        }
        return builder.build();
    }

    /**
     * Retrieves a single block using the Block Node API.
     *
     * @param blockNumber The block number to retrieve
     * @return The SingleBlockResponse from the API
     */
    public static BlockResponse getBlock(
            @NonNull final BlockAccessServiceGrpc.BlockAccessServiceBlockingStub blockAccessStub,
            final long blockNumber) {
        requireNonNull(blockAccessStub);

        BlockRequest request =
                BlockRequest.newBuilder().setBlockNumber(blockNumber).build();
        return blockAccessStub.getBlock(request);
    }

    /**
     * Retrieves a single block using the Block Node API.
     *
     * @return The SingleBlockResponse from the API
     */
    public static BlockResponse getLatestBlock(
            @NonNull final BlockAccessServiceGrpc.BlockAccessServiceBlockingStub blockAccessStub) {
        requireNonNull(blockAccessStub);

        BlockRequest request = BlockRequest.newBuilder().setRetrieveLatest(true).build();
        return blockAccessStub.getBlock(request);
    }
}
