// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.plugintest;

import static java.lang.System.Logger.Level.TRACE;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.junit.jupiter.api.Assertions.fail;

import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.grpc.ServiceInterface.Method;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.converter.ConfigConverter;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.helidon.webserver.http.HttpService;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;
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
public abstract class GrpcPluginTestBase<
                P extends BlockNodePlugin, E extends ExecutorService, S extends ScheduledExecutorService>
        extends PluginTestBase<P, E, S> implements ServiceBuilder {
    private static final long RESPONSE_TIMEOUT_NS = 5_000_000_000L; // 5 seconds

    private record ReqOptions(Optional<String> authority, boolean isProtobuf, boolean isJson, String contentType)
            implements ServiceInterface.RequestOptions {}
    /** The GRPC bytes received from the plugin. */
    protected List<Bytes> fromPluginBytes = new CopyOnWriteArrayList<>();
    /** The pipeline for GRPC bytes to the plugin. */
    protected Pipeline<? super Bytes> toPluginPipe;
    /** The pipeline for GRPC bytes from the plugin. */
    protected Pipeline<Bytes> fromPluginPipe;
    /** The GRPC service interface for the plugin. */
    protected ServiceInterface serviceInterface;
    /// The gRPC method used
    protected Method method;

    protected GrpcPluginTestBase(@NonNull final E executorService, @NonNull final S scheduledExecutorService) {
        super(executorService, scheduledExecutorService);
    }

    /**
     * Start the test fixture with the given plugin and historical block facility.
     *
     * @param plugin the plugin to be tested
     * @param historicalBlockFacility the historical block facility to be used
     */
    public void start(
            @NonNull final P plugin,
            @NonNull final Method method,
            @NonNull final HistoricalBlockFacility historicalBlockFacility) {
        start(plugin, method, historicalBlockFacility, null, null);
    }

    public void start(
            @NonNull final P plugin,
            @NonNull final Method method,
            @NonNull final HistoricalBlockFacility historicalBlockFacility,
            @Nullable final Map<String, String> configOverrides) {
        start(plugin, method, historicalBlockFacility, null, configOverrides);
    }

    public void start(
            @NonNull final P plugin,
            @NonNull final Method method,
            @NonNull final HistoricalBlockFacility historicalBlockFacility,
            @Nullable final List<BlockNodePlugin> additionalPlugins) {
        start(plugin, method, historicalBlockFacility, additionalPlugins, null);
    }

    public void start(
            @NonNull final P plugin,
            @NonNull final Method method,
            @NonNull final HistoricalBlockFacility historicalBlockFacility,
            @Nullable final List<BlockNodePlugin> additionalPlugins,
            @Nullable final Map<String, String> configOverrides) {
        start(plugin, method, historicalBlockFacility, additionalPlugins, configOverrides, Map.of());
    }

    public void start(
            @NonNull final P plugin,
            @NonNull final Method method,
            @NonNull final HistoricalBlockFacility historicalBlockFacility,
            @Nullable final List<BlockNodePlugin> additionalPlugins,
            @Nullable final Map<String, String> configOverrides,
            @Nullable final Map<Class<?>, ConfigConverter<?>> converters) {
        super.start(plugin, historicalBlockFacility, additionalPlugins, configOverrides, converters);
        this.method = Objects.requireNonNull(method);
        setupNewPipelines();
    }

    /// Setup new pipelines to be used.
    protected void setupNewPipelines() {
        final TestPipeline newPipeline = createNewPipeline();
        fromPluginPipe = newPipeline.fromPluginPipe;
        toPluginPipe = newPipeline.toPluginPipe;
        fromPluginBytes = newPipeline.fromPluginBytes;
    }

    /// Setup new pipelines to be used.
    protected TestPipeline createNewPipeline() {
        final List<Bytes> bytes = new CopyOnWriteArrayList<>();
        // setup to receive bytes from the plugin
        final Pipeline<Bytes> fromPipe = new Pipeline<>() {
            @Override
            public void clientEndStreamReceived() {
                LOGGER.log(TRACE, "clientEndStreamReceived");
            }

            @Override
            public void onNext(Bytes item) throws RuntimeException {
                bytes.add(item);
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
        final Pipeline<? super Bytes> toPipe = serviceInterface.open(
                Objects.requireNonNull(method),
                new ReqOptions(Optional.empty(), true, false, "application/grpc"),
                fromPipe);
        return new TestPipeline(toPipe, fromPipe, bytes);
    }

    @Override
    public void registerHttpService(String path, HttpService... service) {
        fail("Register http service not expected: " + path);
    }

    @Override
    public void registerGrpcService(@NonNull ServiceInterface service) {
        serviceInterface = service;
    }

    /// Polls until `fromPluginBytes` reaches the expected size or the
    /// 5-second timeout expires. Uses short polling intervals instead of a
    /// fixed sleep to avoid timing-based test flakiness.
    protected void awaitPluginResponses(final int expectedCount) {
        final long deadline = System.nanoTime() + RESPONSE_TIMEOUT_NS;
        while (fromPluginBytes.size() < expectedCount && System.nanoTime() < deadline) {
            parkNanos(1_000_000L);
        }
    }

    /// Polls until all receivers reach the expected size or the
    /// 5-second timeout expires. Uses short polling intervals instead of a
    /// fixed sleep to avoid timing-based test flakiness.
    protected void awaitPluginResponses(final List<List<Bytes>> receivers, final int expectedCount) {
        final long deadline = System.nanoTime() + RESPONSE_TIMEOUT_NS;
        final Predicate<List<List<Bytes>>> expectedCountReceivedPredicate = r -> {
            for (final List<Bytes> receiver : receivers) {
                if (receiver.size() < expectedCount) {
                    return true;
                }
            }
            return false;
        };
        while (expectedCountReceivedPredicate.test(receivers) && System.nanoTime() < deadline) {
            parkNanos(1_000_000L);
        }
    }

    protected record TestPipeline(
            Pipeline<? super Bytes> toPluginPipe, Pipeline<Bytes> fromPluginPipe, List<Bytes> fromPluginBytes) {}
}
