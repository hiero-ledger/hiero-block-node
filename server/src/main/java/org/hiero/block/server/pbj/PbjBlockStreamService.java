// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.pbj;

import static org.hiero.block.server.Constants.FULL_SERVICE_NAME_BLOCK_STREAM;
import static org.hiero.block.server.Constants.SERVICE_NAME_BLOCK_STREAM;

import com.hedera.pbj.runtime.grpc.ServiceInterface;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Arrays;
import java.util.List;

/**
 * The PbjBlockStreamService interface provides type definitions and default method
 * implementations for PBJ to route gRPC requests to the block stream services.
 */
public interface PbjBlockStreamService extends ServiceInterface {

    /**
     * BlockStreamMethod types define the gRPC methods available on the BlockStreamService.
     */
    enum BlockStreamMethod implements Method {
        /**
         * The publishBlockStream method represents the bidirectional gRPC streaming method
         * Consensus Nodes should use to publish the BlockStream to the Block Node.
         */
        publishBlockStream,

        /**
         * The subscribeBlockStream method represents the server-streaming gRPC method
         * consumers should use to subscribe to the BlockStream from the Block Node.
         */
        subscribeBlockStream
    }

    /**
     * Streams the block item.
     *
     * @return the block item
     */
    @NonNull
    default String serviceName() {
        return SERVICE_NAME_BLOCK_STREAM;
    }

    /**
     * Provides the full name of the BlockStreamService.
     *
     * @return the full name of the BlockStreamService.
     */
    @NonNull
    default String fullName() {
        return FULL_SERVICE_NAME_BLOCK_STREAM;
    }

    /**
     * Provides the methods of the methods on the BlockStreamService.
     *
     * @return the methods of the BlockStreamService.
     */
    @NonNull
    default List<Method> methods() {
        return Arrays.asList(BlockStreamMethod.values());
    }
}
