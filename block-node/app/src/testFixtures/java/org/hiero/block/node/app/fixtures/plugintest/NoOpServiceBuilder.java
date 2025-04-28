// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.plugintest;

import com.hedera.pbj.runtime.grpc.ServiceInterface;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.webserver.http.HttpService;
import org.hiero.block.node.spi.ServiceBuilder;

/**
 * A simple no-op implementation of {@link ServiceBuilder} that does nothing.
 * To be used for testing purposes only where we need a non-null implementation
 * that we do not want to act upon.
 */
public final class NoOpServiceBuilder implements ServiceBuilder {
    /** No-op implementation, does nothing. */
    @Override
    public void registerHttpService(final String path, final HttpService... service) {}

    /** No-op implementation, does nothing. */
    @Override
    public void registerGrpcService(@NonNull final ServiceInterface service) {}
}
