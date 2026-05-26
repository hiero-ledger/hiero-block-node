// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi;

import com.hedera.pbj.runtime.grpc.ServiceInterface;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.webserver.http.HttpService;

/**
 * ServiceBuilder is an interface that defines the contract for registering HTTP and gRPC services
 * with the web server during initialization.
 *
 * <p>Each registration method accepts a port number. The runtime creates one
 * {@link io.helidon.webserver.WebServer} per distinct port, so registering all services on the same
 * port automatically results in a single listener with all routes merged. Plugins obtain the
 * appropriate port from their {@link BlockNodeContext} configuration (e.g.
 * {@code context.configuration().getConfigData(ServerConfig.class).port()}).
 */
public interface ServiceBuilder {

    /**
     * Registers an HTTP service on the given port.
     *
     * @param path the path for the HTTP service
     * @param port the port number to bind this service to
     * @param service the HTTP services to register at that path
     */
    void registerHttpService(@NonNull String path, int port, @NonNull HttpService... service);

    /**
     * Registers a gRPC service on the given port.
     *
     * @param service the gRPC service to register
     * @param port the port number to bind this service to
     */
    void registerGrpcService(@NonNull ServiceInterface service, int port);
}
