// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import com.hedera.pbj.grpc.helidon.PbjRouting;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.webserver.http.HttpRouting;
import io.helidon.webserver.http.HttpService;
import java.util.EnumMap;
import java.util.Map;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.ServiceBuilder.Socket;

/**
 * Default implementation of {@link ServiceBuilder}. That builds HTTP and PBJ GRPC services.
 * <p>
 * Services are bucketed by {@link Socket} ({@link Socket#PUBLISHER} or {@link Socket#CONSUMER}).
 * {@link BlockNodeApp} retrieves the per-socket builders and wires them to the appropriate
 * {@link io.helidon.webserver.WebServer} instances.
 */
public class ServiceBuilderImpl implements ServiceBuilder {
    /** Per-socket HTTP routing builders. */
    private final Map<Socket, HttpRouting.Builder> httpBuilders = new EnumMap<>(Socket.class);
    /** Per-socket PBJ gRPC routing builders. */
    private final Map<Socket, PbjRouting.Builder> grpcBuilders = new EnumMap<>(Socket.class);

    /**
     * {@inheritDoc}
     */
    @Override
    public void registerHttpService(@NonNull String path, @NonNull HttpService... service) {
        httpBuilders
                .computeIfAbsent(Socket.CONSUMER, k -> HttpRouting.builder())
                .register(path, service);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void registerGrpcService(@NonNull ServiceInterface service, @NonNull Socket socket) {
        grpcBuilders.computeIfAbsent(socket, k -> PbjRouting.builder()).service(service);
    }

    /**
     * Returns all HTTP routing builders keyed by socket.
     *
     * @return map of {@link Socket} to {@link HttpRouting.Builder}
     */
    Map<Socket, HttpRouting.Builder> httpRoutingBuilders() {
        return httpBuilders;
    }

    /**
     * Returns all gRPC routing builders keyed by socket.
     *
     * @return map of {@link Socket} to {@link PbjRouting.Builder}
     */
    Map<Socket, PbjRouting.Builder> grpcRoutingBuilders() {
        return grpcBuilders;
    }
}
