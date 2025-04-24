// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.utils;

import static java.util.Objects.requireNonNull;

import edu.umd.cs.findbugs.annotations.NonNull;
import org.hiero.block.api.protoc.BlockAccessServiceGrpc;
import org.hiero.block.api.protoc.BlockRequest;
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
    public static BlockRequest createSingleBlockRequest(long blockNumber, boolean latest) {
        return BlockRequest.newBuilder()
                .setBlockNumber(blockNumber)
                .setRetrieveLatest(latest)
                .setAllowUnverified(true)
                .build();
    }

    /**
     * Retrieves a single block using the Block Node API.
     *
     * @param blockNumber The block number to retrieve
     * @param allowUnverified A flag to indicate that the requested block may be sent without
     *   verifying its `BlockProof`
     * @return The SingleBlockResponse from the API
     */
    public static BlockResponse getSingleBlock(
            @NonNull final BlockAccessServiceGrpc.BlockAccessServiceBlockingStub blockAccessStub,
            final long blockNumber,
            final boolean allowUnverified) {
        requireNonNull(blockAccessStub);

        BlockRequest request = BlockRequest.newBuilder()
                .setBlockNumber(blockNumber)
                .setAllowUnverified(allowUnverified)
                .build();
        return blockAccessStub.getBlock(request);
    }

    /**
     * Retrieves a single block using the Block Node API.
     *
     * @param allowUnverified A flag to indicate that the requested block may be sent without
     * verifying its `BlockProof`
     * @return The SingleBlockResponse from the API
     */
    public static BlockResponse getLatestBlock(
            @NonNull final BlockAccessServiceGrpc.BlockAccessServiceBlockingStub blockAccessStub,
            final boolean allowUnverified) {
        requireNonNull(blockAccessStub);

        BlockRequest request = BlockRequest.newBuilder()
                .setBlockNumber(-1)
                .setRetrieveLatest(true)
                .setAllowUnverified(allowUnverified)
                .build();
        return blockAccessStub.getBlock(request);
    }
}
