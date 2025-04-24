// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.plugintest;

import static java.lang.System.Logger.Level.TRACE;
import static org.junit.jupiter.api.Assertions.fail;

import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.grpc.ServiceInterface.Method;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.webserver.http.HttpService;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Flow.Subscription;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;

/**
 * Base class for testing GRPC block node plugins.
 * <p>
 * This class sets up a GRPC connection to the plugin, so that you can send and receive GRPC protobuf objects to the
 * plugin. It's parent PluginTestBase mocks out all the base functionality of the block node, including the
 * configuration, metrics, health, block messaging, and historical block facilities. See {@link PluginTestBase} for
 * more details.
 * <p>
 * Implementations of this class should call one of the start() methods. This will start the plugin and initialize the
 * test fixture.
 */
public abstract class GrpcPluginTestBase<P extends BlockNodePlugin> extends PluginTestBase<P>
        implements ServiceBuilder {
    private record ReqOptions(Optional<String> authority, boolean isProtobuf, boolean isJson, String contentType)
            implements ServiceInterface.RequestOptions {}
    /** The GRPC bytes received from the plugin. */
    protected List<Bytes> fromPluginBytes = new ArrayList<>();
    /** The pipeline for GRPC bytes to the plugin. */
    protected Pipeline<? super Bytes> toPluginPipe;
    /** The pipeline for GRPC bytes from the plugin. */
    protected Pipeline<Bytes> fromPluginPipe;
    /** The GRPC service interface for the plugin. */
    protected ServiceInterface serviceInterface;

    /**
     * Start the test fixture with the given plugin and historical block facility.
     *
     * @param plugin the plugin to be tested
     * @param historicalBlockFacility the historical block facility to be used
     */
    public void start(P plugin, Method method, HistoricalBlockFacility historicalBlockFacility) {
        super.start(plugin, historicalBlockFacility);
        // setup to receive bytes from the plugin
        fromPluginPipe = new Pipeline<>() {
            @Override
            public void clientEndStreamReceived() {
                LOGGER.log(TRACE, "clientEndStreamReceived");
            }

            @Override
            public void onNext(Bytes item) throws RuntimeException {
                fromPluginBytes.add(item);
                LOGGER.log(TRACE, "onNext: %d".formatted(fromPluginBytes.size()));
            }

            @Override
            public void onSubscribe(Subscription subscription) {
                LOGGER.log(TRACE, "onSubscribe");
            }

            @Override
            public void onError(Throwable throwable) {
                fail("onError: ", throwable);
            }

            @Override
            public void onComplete() {
                LOGGER.log(TRACE, "onComplete");
            }
        };
        // open a fake GRPC connection to the plugin
        toPluginPipe = serviceInterface.open(
                method, new ReqOptions(Optional.empty(), true, false, "application/grpc"), fromPluginPipe);
    }

    @Override
    public void registerHttpService(String path, HttpService... service) {
        fail("Register http service not expected: " + path);
    }

    @Override
    public void registerGrpcService(@NonNull ServiceInterface service) {
        serviceInterface = service;
    }
}
