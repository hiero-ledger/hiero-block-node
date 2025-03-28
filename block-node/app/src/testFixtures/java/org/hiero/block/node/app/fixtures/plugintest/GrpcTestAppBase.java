// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.plugintest;

import static org.junit.jupiter.api.Assertions.fail;

import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.grpc.ServiceInterface.Method;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.common.metrics.platform.DefaultMetricsProvider;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.metrics.api.Metrics;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.webserver.http.HttpService;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Flow.Subscription;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.health.HealthFacility;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.junit.jupiter.api.AfterEach;

/**
 * Base class for testing GRPC apps.
 * <p>
 * This class provides a mock implementation of the BlockNodePlugin interface and sets up a GRPC connection to the
 * plugin. It mocks out all the base functionality of the block node, including the configuration, metrics, health,
 * block messaging, and historical block facilities.
 * <p>
 * The messaging services is mocked out with two concurrent queues, one for block items
 * {@link GrpcTestAppBase:sentBlockBlockItems} and one for block notifications
 * {@link GrpcTestAppBase:sentBlockNotifications}. You can look at these queues to see what was sent to the messaging
 * service by the plugin.
 */
public abstract class GrpcTestAppBase implements ServiceBuilder {
    private record ReqOptions(Optional<String> authority, boolean isProtobuf, boolean isJson, String contentType)
            implements ServiceInterface.RequestOptions {}

    private final DefaultMetricsProvider metricsProvider;
    protected final BlockNodeContext blockNodeContext;
    protected final ConcurrentLinkedQueue<Bytes> fromPluginBytes = new ConcurrentLinkedQueue<>();
    protected final Pipeline<? super Bytes> toPluginPipe;
    protected final TestBlockMessagingFacility blockMessaging = new TestBlockMessagingFacility();
    protected ServiceInterface serviceInterface;

    public GrpcTestAppBase(BlockNodePlugin plugin, Method method, HistoricalBlockFacility historicalBlockFacility) {
        // Build the configuration
        //noinspection unchecked
        final Configuration configuration = ConfigurationBuilder.create()
                .withConfigDataType(com.swirlds.common.metrics.config.MetricsConfig.class)
                .withConfigDataType(com.swirlds.common.metrics.platform.prometheus.PrometheusConfig.class)
                .withConfigDataTypes(plugin.configDataTypes().toArray(new Class[0]))
                .build();
        // create metrics provider
        metricsProvider = new DefaultMetricsProvider(configuration);
        final Metrics metrics = metricsProvider.createGlobalMetrics();
        metricsProvider.start();
        // mock health facility
        final HealthFacility healthFacility = new AllwaysRunningHealthFacility();
        // create block node context
        blockNodeContext = new BlockNodeContext() {
            @Override
            public Configuration configuration() {
                return configuration;
            }

            @Override
            public Metrics metrics() {
                return metrics;
            }

            @Override
            public HealthFacility serverHealth() {
                return healthFacility;
            }

            @Override
            public BlockMessagingFacility blockMessaging() {
                return blockMessaging;
            }

            @Override
            public HistoricalBlockFacility historicalBlockProvider() {
                return historicalBlockFacility;
            }
        };
        // start plugin
        plugin.init(blockNodeContext, this);
        plugin.start();
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

    @AfterEach
    public void tearDown() {
        metricsProvider.stop();
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
