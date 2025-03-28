// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.publisher;

import static org.hiero.block.node.publisher.PublisherServicePlugin.BlockStreamPublisherServiceMethod.publishBlockStream;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import com.hedera.hapi.block.BlockItemSet;
import com.hedera.hapi.block.PublishStreamRequest;
import com.hedera.hapi.block.PublishStreamRequest.RequestOneOfType;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.BlockItem.ItemOneOfType;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.input.RoundHeader;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.BlockHashAlgorithm;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.OneOf;
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
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Flow.Subscription;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockItemHandler;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.NoBackPressureBlockItemHandler;
import org.hiero.block.node.spi.health.HealthFacility;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the PublisherServicePlugin. It mocks out the rest of the block node so we can simply test just this plugin.
 */
@SuppressWarnings({"FieldCanBeLocal", "MismatchedQueryAndUpdateOfCollection", "SameParameterValue"})
public class PublisherTest implements ServiceBuilder {
    private record ReqOptions(Optional<String> authority, boolean isProtobuf, boolean isJson, String contentType)
            implements ServiceInterface.RequestOptions {}

    private static BlockItem sampleBlockHeader(long blockNumber) {
        return new BlockItem(new OneOf<>(
                ItemOneOfType.BLOCK_HEADER,
                new BlockHeader(
                        new SemanticVersion(1, 2, 3, "a", "b"),
                        new SemanticVersion(4, 5, 6, "c", "d"),
                        blockNumber,
                        Bytes.wrap("hash".getBytes()),
                        new Timestamp(123L, 456),
                        BlockHashAlgorithm.SHA2_384)));
    }

    private static BlockItem sampleRoundHeader(long roundNumber) {
        return new BlockItem(new OneOf<>(ItemOneOfType.ROUND_HEADER, new RoundHeader(roundNumber)));
    }

    private static BlockItem sampleBlockProof(long blockNumber) {
        return new BlockItem(new OneOf<>(
                ItemOneOfType.BLOCK_PROOF,
                new BlockProof(
                        blockNumber,
                        Bytes.wrap("previousBlockRootHash".getBytes()),
                        Bytes.wrap("startOfBlockStateRootHash".getBytes()),
                        Bytes.wrap("signature".getBytes()),
                        Collections.emptyList())));
    }

    private PublisherServicePlugin plugin;
    private DefaultMetricsProvider metricsProvider;
    private BlockNodeContext blockNodeContext;
    private final ConcurrentLinkedQueue<BlockItems> sentBlockBlockItems = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<BlockNotification> sentBlockNotifications = new ConcurrentLinkedQueue<>();
    private ServiceInterface serviceInterface;
    private ConcurrentLinkedQueue<Bytes> fromPluginBytes;
    private Pipeline<? super Bytes> toPluginPipe;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setup() {
        plugin = new PublisherServicePlugin();
        // Build the configuration
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
        final HealthFacility healthFacility = new HealthFacility() {
            @Override
            public State blockNodeState() {
                return State.RUNNING;
            }

            @Override
            public void shutdown(String className, String reason) {
                fail("Shutdown not expected: " + className + " " + reason);
            }
        };
        // mock block messaging facility
        final BlockMessagingFacility blockMessaging = new BlockMessagingFacility() {

            @Override
            public void sendBlockItems(BlockItems blockItems) {
                sentBlockBlockItems.add(blockItems);
            }

            @Override
            public void registerBlockItemHandler(
                    BlockItemHandler handler, boolean cpuIntensiveHandler, String handlerName) {
                fail("Register block item handler not expected: " + handlerName);
            }

            @Override
            public void registerNoBackpressureBlockItemHandler(
                    NoBackPressureBlockItemHandler handler, boolean cpuIntensiveHandler, String handlerName) {
                fail("Register no backpressure block item handler not expected: " + handlerName);
            }

            @Override
            public void unregisterBlockItemHandler(BlockItemHandler handler) {
                fail("Unregister block item handler not expected: " + handler);
            }

            @Override
            public void sendBlockNotification(BlockNotification notification) {
                sentBlockNotifications.add(notification);
            }

            @Override
            public void registerBlockNotificationHandler(
                    BlockNotificationHandler handler, boolean cpuIntensiveHandler, String handlerName) {
                fail("Register block notification handler not expected: " + handlerName);
            }

            @Override
            public void unregisterBlockNotificationHandler(BlockNotificationHandler handler) {
                fail("Unregister block notification handler not expected: " + handler);
            }
        };
        // mock historical block facility
        final HistoricalBlockFacility historicalBlockFacility = new HistoricalBlockFacility() {
            @Override
            public BlockAccessor block(long blockNumber) {
                return null;
            }

            @Override
            public long oldestBlockNumber() {
                return UNKNOWN_BLOCK_NUMBER;
            }

            @Override
            public long latestBlockNumber() {
                return UNKNOWN_BLOCK_NUMBER;
            }
        };
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
        fromPluginBytes = new ConcurrentLinkedQueue<>();
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
                publishBlockStream, new ReqOptions(Optional.empty(), true, false, "application/grpc"), fromPluginPipe);
    }

    @AfterEach
    void tearDown() {
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

    @Test
    void testServiceInterfaceBasics() {
        // check we have a service interface
        assertNotNull(serviceInterface);
        // check the methods from service interface
        List<Method> methods = serviceInterface.methods();
        assertNotNull(methods);
        assertEquals(1, methods.size());
        assertEquals(publishBlockStream, methods.getFirst());
    }

    @Test
    void testPublisher() {
        // enable debug System.logger logging
        Logger rootLogger = LogManager.getLogManager().getLogger("");
        rootLogger.setLevel(Level.ALL);
        for (var handler : rootLogger.getHandlers()) {
            handler.setLevel(Level.ALL);
        }
        // create some sample data to send to plugin
        final BlockItem blockHeader1 = sampleBlockHeader(0);
        final Bytes publishBlockHeader1StreamRequest = PublishStreamRequest.PROTOBUF.toBytes(new PublishStreamRequest(
                new OneOf<>(RequestOneOfType.BLOCK_ITEMS, new BlockItemSet(List.of(blockHeader1)))));
        final BlockItem roundHeader1 = sampleRoundHeader(2);
        final Bytes publishRoundHeader1StreamRequest = PublishStreamRequest.PROTOBUF.toBytes(new PublishStreamRequest(
                new OneOf<>(RequestOneOfType.BLOCK_ITEMS, new BlockItemSet(List.of(roundHeader1)))));
        final BlockItem blockProof1 = sampleBlockProof(0);
        final Bytes publishBlockProof1StreamRequest = PublishStreamRequest.PROTOBUF.toBytes(new PublishStreamRequest(
                new OneOf<>(RequestOneOfType.BLOCK_ITEMS, new BlockItemSet(List.of(blockProof1)))));
        // send the data to the plugin
        toPluginPipe.onNext(publishBlockHeader1StreamRequest);
        toPluginPipe.onNext(publishRoundHeader1StreamRequest);
        toPluginPipe.onNext(publishBlockProof1StreamRequest);
        System.out.println("PublisherTest.testPublisher: sent publishBlockHeader1StreamRequest");
    }
}
