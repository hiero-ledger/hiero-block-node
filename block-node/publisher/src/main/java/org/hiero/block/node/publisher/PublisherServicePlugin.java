// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.publisher;

import com.hedera.pbj.grpc.helidon.PbjRouting;
import com.hedera.pbj.runtime.grpc.GrpcException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.common.Builder;
import io.helidon.webserver.Routing;
import java.util.Arrays;
import java.util.List;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;

/** Provides implementation for the block stream publisher endpoints of the server. */
public class PublisherServicePlugin implements BlockNodePlugin, ServiceInterface {

    /**
     * BlockStreamPublisherService types define the gRPC methods available on the BlockStreamPublisherService.
     */
    enum BlockStreamPublisherServiceMethod implements Method {
        /**
         * The publishBlockStream method represents the bidirectional gRPC streaming method
         * Consensus Nodes should use to publish the BlockStream to the Block Node.
         */
        publishBlockStream
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String name() {
        return "Publisher Service Plugin";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Builder<?, ? extends Routing> init(BlockNodeContext context) {
        // register us as a service
        return PbjRouting.builder().service(this);
    }

    // ==== ServiceInterface Methods ===================================================================================

    /**
     * {@inheritDoc}
     */
    @NonNull
    public String serviceName() {
        return "BlockStreamPublisherService";
    }

    /**
     * {@inheritDoc}
     */
    @NonNull
    public String fullName() {
        return "com.hedera.hapi.block.node." + serviceName();
    }

    /**
     * {@inheritDoc}
     */
    @NonNull
    public List<Method> methods() {
        return Arrays.asList(BlockStreamPublisherServiceMethod.values());
    }

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public Pipeline<? super Bytes> open(
            @NonNull Method method, @NonNull RequestOptions opts, @NonNull Pipeline<? super Bytes> responses)
            throws GrpcException {
        // TODO: port implementation from org.hiero.block.server.pbj.PbjBlockStreamServiceProxy
        return null;
    }
}
