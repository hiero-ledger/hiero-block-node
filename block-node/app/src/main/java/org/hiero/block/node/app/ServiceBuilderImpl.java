// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import com.hedera.pbj.grpc.helidon.PbjRouting;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import edu.umd.cs.findbugs.annotations.NonNull;
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
 */
public class ServiceBuilderImpl implements ServiceBuilder {
    /** Per-port HTTP routing builders. */
    private final Map<Integer, HttpRouting.Builder> httpBuilders = new HashMap<>();
    /** Per-port PBJ gRPC routing builders. */
    private final Map<Integer, PbjRouting.Builder> grpcBuilders = new HashMap<>();

    /**
     * {@inheritDoc}
     */
    @Override
    public void registerHttpService(@NonNull String path, int port, @NonNull HttpService... service) {
        httpBuilders.computeIfAbsent(port, k -> HttpRouting.builder()).register(path, service);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void registerGrpcService(@NonNull ServiceInterface service, int port) {
        grpcBuilders.computeIfAbsent(port, k -> PbjRouting.builder()).service(service);
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
}
