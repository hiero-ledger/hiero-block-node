// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.plugintest;

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
 */
public abstract class GrpcPluginTestBase extends PluginTestBase implements ServiceBuilder {
    private record ReqOptions(Optional<String> authority, boolean isProtobuf, boolean isJson, String contentType)
            implements ServiceInterface.RequestOptions {}

    protected final List<Bytes> fromPluginBytes = new ArrayList<>();
    protected final Pipeline<? super Bytes> toPluginPipe;
    protected ServiceInterface serviceInterface;

    public GrpcPluginTestBase(BlockNodePlugin plugin, Method method, HistoricalBlockFacility historicalBlockFacility) {
        super(plugin, historicalBlockFacility);
        // setup to receive bytes from the plugin
        final Pipeline<Bytes> fromPluginPipe = new Pipeline<>() {
            @Override
            public void clientEndStreamReceived() {
                System.out.println("PublisherTest.clientEndStreamReceived");
            }

            @Override
            public void onNext(Bytes item) throws RuntimeException {
                fromPluginBytes.add(item);
            }

            @Override
            public void onSubscribe(Subscription subscription) {
                System.out.println("PublisherTest.onSubscribe");
            }

            @Override
            public void onError(Throwable throwable) {
                fail("PublisherTest.onError: ", throwable);
            }

            @Override
            public void onComplete() {
                System.out.println("PublisherTest.onComplete");
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
