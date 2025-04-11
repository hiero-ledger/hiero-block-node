// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import com.hedera.pbj.grpc.helidon.PbjRouting;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.webserver.http.HttpRouting;
import io.helidon.webserver.http.HttpRouting.Builder;
import io.helidon.webserver.http.HttpService;
import org.hiero.block.node.spi.ServiceBuilder;

/**
 * Default implementation of {@link ServiceBuilder}. That builds HTTP and PBJ GRPC services.
 * <p>
 * This class is used to register services with the block node.
 */
public class ServiceBuilderImpl implements ServiceBuilder {
    /** The HTTP routing builder. */
    private final HttpRouting.Builder httpRoutingBuilder = HttpRouting.builder();
    /** The PBJ GRPC routing builder. */
    private final PbjRouting.Builder pbjRoutingBuilder = PbjRouting.builder();

    /**
     * {@inheritDoc}
     */
    @Override
    public void registerHttpService(String path, HttpService... service) {
        httpRoutingBuilder.register(path, service);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void registerGrpcService(@NonNull ServiceInterface service) {
        pbjRoutingBuilder.service(service);
    }

    /**
     * Returns the HTTP routing builder.
     *
     * @return the HTTP routing builder
     */
    Builder httpRoutingBuilder() {
        return httpRoutingBuilder;
    }

    /**
     * Returns the GRPC routing builder.
     *
     * @return the GRPC routing builder
     */
    PbjRouting.Builder grpcRoutingBuilder() {
        return pbjRoutingBuilder;
    }
}
