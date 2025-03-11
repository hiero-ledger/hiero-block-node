// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.pbj;

import static com.hedera.block.server.Constants.FULL_SERVICE_NAME_BLOCK_NODE;
import static com.hedera.block.server.Constants.SERVICE_NAME_BLOCK_NODE;

import com.hedera.pbj.runtime.grpc.ServiceInterface;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Arrays;
import java.util.List;

/**
 * The PbjServerStatusService interface provides type definitions and default method
 * implementations for PBJ to route gRPC requests to the server status service.
 */
public interface PbjServerStatusService extends ServiceInterface {

    /**
     * ServerStatusMethod types define the gRPC methods available on the ServerStatusService.
     */
    enum ServerStatusMethod implements Method {
        /**
         * The serverStatus method represents the unary gRPC method
         * consumers should use to retrieve the status of the Block Node.
         */
        serverStatus
    }

    /**
     * Provides the service name of the ServerStatusService.
     *
     * @return the service name of the ServerStatusService.
     */
    @NonNull
    default String serviceName() {
        return SERVICE_NAME_BLOCK_NODE;
    }

    /**
     * Provides the full name of the ServerStatusService.
     *
     * @return the full name of the ServerStatusService.
     */
    @NonNull
    default String fullName() {
        return FULL_SERVICE_NAME_BLOCK_NODE;
    }

    /**
     * Provides the list of methods available in the ServerStatusService.
     *
     * @return the list of available methods in the ServerStatusService.
     */
    @NonNull
    default List<Method> methods() {
        return Arrays.asList(PbjServerStatusService.ServerStatusMethod.values());
    }
}
