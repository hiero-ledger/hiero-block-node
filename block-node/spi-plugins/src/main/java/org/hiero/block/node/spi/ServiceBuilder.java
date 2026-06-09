// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi;

import com.hedera.pbj.runtime.grpc.ServiceInterface;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.helidon.webserver.http.HttpService;

/**
 * ServiceBuilder is an interface that defines the contract for registering HTTP and gRPC services
 * with the web server during initialization.
 *
 * <p>Each registration method accepts an optional port number. When {@code port} is {@code null}
 * the runtime binds the service to the default server port ({@code server.port}). Plugins that
 * need their own dedicated port obtain it from their own {@link BlockNodeContext} configuration
 * and pass it explicitly; plugins that are happy sharing the default port simply pass {@code null}.
 */
public interface ServiceBuilder {

    /**
     * Registers an HTTP service on the given port, or on the default port if {@code port} is {@code null}.
     *
     * @param path the path for the HTTP service
     * @param port the port number to bind this service to, or {@code null} to use the default port
     * @param service the HTTP services to register at that path
     */
    void registerHttpService(@NonNull String path, @Nullable Integer port, @NonNull HttpService... service);

    /**
     * Registers a gRPC service on the given port, or on the default port if {@code port} is {@code null}.
     *
     * @param service the gRPC service to register
     * @param port the port number to bind this service to, or {@code null} to use the default port
     */
    void registerGrpcService(@NonNull ServiceInterface service, @Nullable Integer port);
}
