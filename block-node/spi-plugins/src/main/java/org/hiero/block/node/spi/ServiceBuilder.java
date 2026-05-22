// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi;

import com.hedera.pbj.runtime.grpc.ServiceInterface;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.webserver.http.HttpService;

/**
 * ServiceBuilder is an interface that defines the contract for registering HTTP and gRPC services with the web server
 * during initialization.
 *
 * <p>Services are registered against a {@link Socket}. Two sockets are defined:
 * <ul>
 *   <li>{@link Socket#PUBLISHER} — for inbound block-stream publishing (CN → BN)</li>
 *   <li>{@link Socket#CONSUMER} — for all other services (subscribers, block access, health)</li>
 * </ul>
 * When both sockets share the same port the implementation merges them onto a single listener.
 * Every registration requires an explicit {@link Socket} — use {@link Socket#CONSUMER} for all
 * services except the CN -> BN publisher, which uses {@link Socket#PUBLISHER}.
 */
public interface ServiceBuilder {
    /** Identifies which network socket a service should be bound to. */
    enum Socket {
        /** Publisher-side socket for inbound block-stream publishing (CN -> BN). */
        PUBLISHER,
        /** Consumer-side socket for subscribers, health checks, and block access. */
        CONSUMER
    }

    /**
     * Registers an HTTP service on the {@link Socket#CONSUMER} socket.
     *
     * @param path the path for the HTTP service
     * @param service the HTTP services to register at that path
     */
    void registerHttpService(@NonNull String path, @NonNull HttpService... service);

    /**
     * Registers a gRPC service on the given socket.
     *
     * @param service the gRPC service to register
     * @param socket the target socket ({@link Socket#PUBLISHER} or {@link Socket#CONSUMER})
     */
    void registerGrpcService(@NonNull ServiceInterface service, @NonNull Socket socket);
}
