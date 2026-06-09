// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import com.hedera.pbj.grpc.helidon.PbjRouting;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.helidon.webserver.http.HttpRouting;
import io.helidon.webserver.http.HttpService;
import java.util.HashMap;
import java.util.Map;
import org.hiero.block.node.spi.ServiceBuilder;

/**
 * Default implementation of {@link ServiceBuilder}. That builds HTTP and PBJ GRPC services.
 * <p>
 * Services are bucketed by port number. {@link BlockNodeApp} creates one
 * {@link io.helidon.webserver.WebServer} per distinct port found in the maps, so registering all
 * services on the same port results in a single listener with all routes merged.
 * <p>
 * A {@code null} port in any registration call resolves to the default port supplied at
 * construction time (typically {@code server.port}).
 */
public class ServiceBuilderImpl implements ServiceBuilder {
    /** The port used when a plugin passes {@code null}. */
    private final int defaultPort;
    /** Per-port HTTP routing builders. */
    private final Map<Integer, HttpRouting.Builder> httpBuilders = new HashMap<>();
    /** Per-port PBJ gRPC routing builders. */
    private final Map<Integer, PbjRouting.Builder> grpcBuilders = new HashMap<>();

    /**
     * Creates a new instance with the given default port.
     *
     * @param defaultPort the port used when a plugin passes {@code null}
     */
    ServiceBuilderImpl(int defaultPort) {
        this.defaultPort = defaultPort;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void registerHttpService(@NonNull String path, @Nullable Integer port, @NonNull HttpService... service) {
        httpBuilders.computeIfAbsent(resolve(port), k -> HttpRouting.builder()).register(path, service);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void registerGrpcService(@NonNull ServiceInterface service, @Nullable Integer port) {
        grpcBuilders.computeIfAbsent(resolve(port), k -> PbjRouting.builder()).service(service);
    }

    /**
     * Returns all HTTP routing builders keyed by port.
     *
     * @return map of port to {@link HttpRouting.Builder}
     */
    Map<Integer, HttpRouting.Builder> httpRoutingBuilders() {
        return httpBuilders;
    }

    /**
     * Returns all gRPC routing builders keyed by port.
     *
     * @return map of port to {@link PbjRouting.Builder}
     */
    Map<Integer, PbjRouting.Builder> grpcRoutingBuilders() {
        return grpcBuilders;
    }

    private int resolve(@Nullable Integer port) {
        return port != null ? port : defaultPort;
    }
}
