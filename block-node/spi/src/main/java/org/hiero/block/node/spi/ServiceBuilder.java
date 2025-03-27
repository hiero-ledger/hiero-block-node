// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi;

import com.hedera.pbj.runtime.grpc.ServiceInterface;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.webserver.http.HttpService;

/**
 * ServiceBuilder is an interface that defines the contract for registering HTTP and gRPC services with the web server
 * during initialization.
 */
public interface ServiceBuilder {
    /**
     * Registers an HTTP service with the web server.
     *
     * @param path the path for the HTTP service
     * @param service the HTTP service to register
     */
    void registerHttpService(String path, HttpService... service);

    /**
     * Registers a gRPC service with the web server.
     *
     * @param service the gRPC service to register
     */
    void registerGrpcService(@NonNull ServiceInterface service);
}
