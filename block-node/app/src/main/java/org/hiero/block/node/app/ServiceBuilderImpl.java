// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;

import com.hedera.pbj.grpc.helidon.PbjRouting;
import com.hedera.pbj.grpc.helidon.config.PbjConfig;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.helidon.common.socket.SocketOptions;
import io.helidon.webserver.ListenerConfig;
import io.helidon.webserver.WebServerConfig;
import io.helidon.webserver.http.HttpRouting;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http2.Http2Config;
import java.net.StandardSocketOptions;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.ToIntFunction;
import org.hiero.block.node.app.config.BlockReadBulkheadConfig;
import org.hiero.block.node.app.config.GlobalThrottleConfig;
import org.hiero.block.node.app.config.ServerConfig;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.bulkhead.BlockReadBulkhead;
import org.hiero.block.node.spi.threading.ThreadPoolManager;
import org.hiero.block.node.spi.throttle.RemoteAddressKeyExtractor;
import org.hiero.block.node.spi.throttle.StaleClientSweepable;
import org.hiero.block.node.spi.throttle.ThrottleExempt;
import org.hiero.block.node.spi.throttle.ThrottleMetrics;
import org.hiero.block.node.spi.throttle.ThrottleSpec;
import org.hiero.block.node.spi.throttle.ThrottledServiceInterface;
import org.hiero.metrics.core.MetricRegistry;

/// Default implementation of [ServiceBuilder]. That builds HTTP and PBJ GRPC services.
///
/// Services are bucketed by port number. [BlockNodeApp] creates one
/// [io.helidon.webserver.WebServer] per distinct port found in the maps, so registering all
/// services on the same port results in a single listener with all routes merged.
///
/// A `null` port in any registration call resolves to the default port supplied at
/// construction time (typically `server.port`).
public class ServiceBuilderImpl implements ServiceBuilder {
    private static final System.Logger LOGGER = System.getLogger(ServiceBuilderImpl.class.getName());

    /** Per-port HTTP routing builders. */
    private final Map<Integer, HttpRouting.Builder> httpBuilders = new HashMap<>();
    /** Per-port PBJ gRPC routing builders. */
    private final Map<Integer, PbjRouting.Builder> grpcBuilders = new HashMap<>();

    private final ServerConfig serverConfig;
    private final Http2Config http2Config;
    private final SocketOptions socketOptions;
    private final GlobalThrottleConfig globalThrottleConfig;
    private final MetricRegistry metricRegistry;
    private final ThreadPoolManager threadPoolManager;
    private WebServerResult generalWebserver;
    private final LinkedHashSet<WebServerResult> additionalWebservers;
    /** The single, shared block-read bulkhead every plugin's [#blockReadBulkhead] call returns. */
    private final BlockReadBulkhead blockReadBulkhead;

    /**
     * Every throttled service this instance has created, for the periodic stale-client sweep and permit
     * reaper. Copy-on-write because registration adds on the startup thread while the executor iterates.
     */
    private final List<StaleClientSweepable> throttledServices = new CopyOnWriteArrayList<>();
    /** Lazily created on the first throttled registration; runs the stale-client-state sweep and permit reaper. */
    private ScheduledExecutorService clientStateSweepExecutor;
    /** The permit reaper's schedule, or null while no registered service reaps. */
    private ScheduledFuture<?> permitReaperFuture;
    /** The permit reaper's current period, 0 while it is not scheduled. */
    private long permitReaperPeriodMillis;
    /** The throttle mechanism's shared, once-registered metrics; see {@link ThrottleMetrics}. */
    private final ThrottleMetrics throttleMetrics;
    /** Shared across every throttled registration; see its own constructor for why. */
    private final RemoteAddressKeyExtractor keyExtractor;

    public ServiceBuilderImpl(
            final ServerConfig serverConfig,
            final Http2Config http2Config,
            final SocketOptions socketOptions,
            final GlobalThrottleConfig globalThrottleConfig,
            final BlockReadBulkheadConfig blockReadBulkheadConfig,
            final MetricRegistry metricRegistry,
            final ThreadPoolManager threadPoolManager) {
        this.serverConfig = serverConfig;
        this.http2Config = http2Config;
        this.socketOptions = socketOptions;
        this.globalThrottleConfig = globalThrottleConfig;
        this.metricRegistry = metricRegistry;
        this.threadPoolManager = threadPoolManager;
        this.throttleMetrics = new ThrottleMetrics(metricRegistry);
        this.keyExtractor = new RemoteAddressKeyExtractor(metricRegistry);
        this.blockReadBulkhead = new BlockReadBulkhead(blockReadBulkheadConfig.permits(), metricRegistry);
        additionalWebservers = new LinkedHashSet<>();
    }

    /// {@inheritDoc}
    @NonNull
    @Override
    public BlockReadBulkhead blockReadBulkhead() {
        return blockReadBulkhead;
    }

    /// {@inheritDoc}
    @Override
    public void registerHttpService(@NonNull String path, @Nullable Integer port, @NonNull HttpService... service) {
        httpBuilders.computeIfAbsent(resolve(port), k -> HttpRouting.builder()).register(path, service);
    }

    /// {@inheritDoc}
    ///
    /// Every registration is logged once, at startup, as throttled ([ThrottleSpec]), exempt
    /// ([ThrottleExempt]), or neither; the last case logs a warning since an admission-control gap
    /// is more likely an oversight than a deliberate choice.
    ///
    /// If `service` also implements [ThrottleSpec], registers the resulting
    /// [ThrottledServiceInterface] in place of the raw service.
    @Override
    public void registerGrpcService(@Nullable Integer port, @NonNull ServiceInterface service) {
        if (service instanceof ThrottleSpec spec) {
            LOGGER.log(INFO, "Registered gRPC service {0}: throttled", service.serviceName());
            registerThrottledGrpcService(port, service, spec);
        } else {
            if (service instanceof ThrottleExempt) {
                LOGGER.log(
                        INFO,
                        "Registered gRPC service {0}: exempt (deliberately not throttled)",
                        service.serviceName());
            } else {
                LOGGER.log(
                        INFO,
                        "Registered gRPC service {0}: NOT throttled, and not marked exempt via ThrottleExempt; "
                                + "confirm this is intentional",
                        service.serviceName());
            }
            grpcBuilders
                    .computeIfAbsent(resolve(port), k -> PbjRouting.builder())
                    .service(service);
        }
    }

    private void registerThrottledGrpcService(
            @Nullable final Integer port, @NonNull final ServiceInterface service, @NonNull final ThrottleSpec spec) {
        final ThrottledServiceInterface throttled = new ThrottledServiceInterface(
                service,
                spec.perClientSettings(),
                spec.defaultPerClientSettings(),
                spec.globalConcurrencyCeilings(),
                keyExtractor,
                throttleMetrics,
                Duration.ofMinutes(globalThrottleConfig.clientStateTtlMinutes()));
        throttledServices.add(throttled);
        ensureClientStateSweepStarted();
        ensurePermitReaperScheduled(throttled.smallestPermitHoldSeconds());
        grpcBuilders.computeIfAbsent(resolve(port), k -> PbjRouting.builder()).service(throttled);
    }

    /// Starts the periodic stale-client-state sweep on first throttled registration, not
    /// unconditionally in the constructor, so a node with no throttled services spawns no sweep thread.
    private void ensureClientStateSweepStarted() {
        if (clientStateSweepExecutor != null) {
            return;
        }
        clientStateSweepExecutor = threadPoolManager.createVirtualThreadScheduledExecutor(
                1, "throttle-client-state-sweep", (thread, throwable) -> {});
        final long intervalMinutes = globalThrottleConfig.clientStateSweepIntervalMinutes();
        clientStateSweepExecutor.scheduleAtFixedRate(
                this::sweepAllThrottledServices, intervalMinutes, intervalMinutes, TimeUnit.MINUTES);
    }

    /// Schedules, or tightens, the permit reaper so it runs about twice per smallest permit hold
    /// limit across all registered services, never faster than once per second. The period follows
    /// the operator's configured limits, so it needs no setting of its own. A service that does not
    /// reap (`smallestPermitHoldSeconds == 0`) leaves the reaper as it is.
    ///
    /// @param smallestPermitHoldSeconds the newly registered service's smallest hold limit, or 0
    private void ensurePermitReaperScheduled(final int smallestPermitHoldSeconds) {
        if (smallestPermitHoldSeconds <= 0) {
            return;
        }
        final long periodMillis = Math.max(1_000L, smallestPermitHoldSeconds * 1_000L / 2);
        if (permitReaperFuture != null && periodMillis >= permitReaperPeriodMillis) {
            return;
        }
        if (permitReaperFuture != null) {
            permitReaperFuture.cancel(false);
        }
        permitReaperPeriodMillis = periodMillis;
        permitReaperFuture = clientStateSweepExecutor.scheduleAtFixedRate(
                this::reapAllExpiredPermits, periodMillis, periodMillis, TimeUnit.MILLISECONDS);
    }

    /// Runs one permit-reaper pass across every throttled service registered so far.
    private void reapAllExpiredPermits() {
        final long now = System.nanoTime();
        final int totalReaped =
                runOnEachService(throttledServices, throttled -> throttled.reapExpiredPermits(now), "Permit reaper");
        if (totalReaped > 0) {
            LOGGER.log(DEBUG, "Permit reaper force-released {0} expired concurrency permits", totalReaped);
        }
    }

    /// Runs one stale-client-state sweep across every throttled service registered so far.
    private void sweepAllThrottledServices() {
        final long now = System.nanoTime();
        final int totalEvicted = runOnEachService(
                throttledServices, throttled -> throttled.sweepStaleClients(now), "Stale-client sweep");
        LOGGER.log(TRACE, "Stale-client sweep evicted {0} client-state entries", totalEvicted);
    }

    /// Runs one periodic pass over each throttled service and sums the results. A failure in one
    /// service is logged and skipped, never thrown: an exception escaping a `scheduleAtFixedRate`
    /// task cancels every later run, and stays hidden in the task's `ScheduledFuture`.
    ///
    /// @param services the throttled services to visit
    /// @param pass the per-service work, returning a count
    /// @param passName names the pass in the failure log
    /// @return the sum of `pass` over every service that did not throw
    static int runOnEachService(
            @NonNull final List<? extends StaleClientSweepable> services,
            @NonNull final ToIntFunction<StaleClientSweepable> pass,
            @NonNull final String passName) {
        int total = 0;
        for (final StaleClientSweepable throttled : services) {
            try {
                total += pass.applyAsInt(throttled);
            } catch (final RuntimeException e) {
                LOGGER.log(WARNING, passName + " failed for a throttled service; continuing with the rest", e);
            }
        }
        return total;
    }

    /// Returns all HTTP routing builders keyed by port.
    ///
    /// @return map of port to [HttpRouting.Builder]
    Map<Integer, HttpRouting.Builder> httpRoutingBuilders() {
        return httpBuilders;
    }

    /// Returns all gRPC routing builders keyed by port.
    ///
    /// @return map of port to [PbjRouting.Builder]
    Map<Integer, PbjRouting.Builder> grpcRoutingBuilders() {
        return grpcBuilders;
    }

    private int resolve(@Nullable Integer port) {
        return port != null ? port : serverConfig.port();
    }

    @Override
    public WebServerResult registerHttpNewServer(
            final TreeMap<Integer, ServiceWithPath[]> services, final CommonSocketValues commonSocketValues) {
        return registerHttpNewServer(services, http2Config, socketOptions, commonSocketValues);
    }

    @Override
    public WebServerResult registerHttpNewServer(
            final TreeMap<Integer, ServiceWithPath[]> services,
            final Http2Config http2Config,
            final SocketOptions socketOptions,
            final CommonSocketValues commonSocketValues) {
        // build a single WebServer with named sockets for each port/path.
        TreeMap<Integer, HttpRouting.Builder> routeBuilders = new TreeMap<>();
        for (Entry<Integer, ServiceWithPath[]> entry : services.entrySet()) {
            final HttpRouting.Builder routingBuilder =
                    routeBuilders.computeIfAbsent(entry.getKey(), ign -> HttpRouting.builder());
            for (ServiceWithPath value : entry.getValue()) {
                routingBuilder.register(value.path(), value.services());
            }
        }
        // use defaults for now, but allow for future change if needed.
        final PbjConfig pbjConfig = PbjConfig.builder()
                .name(PBJ_PROTOCOL_PROVIDER_CONFIG_NAME)
                .maxMessageSizeBytes(serverConfig.maxMessageSizeBytes())
                .build();
        WebServerResult serverCreated = buildWebServer(
                services.keySet(), routeBuilders, http2Config, socketOptions, commonSocketValues, pbjConfig);
        additionalWebservers.add(serverCreated);
        return serverCreated;
    }

    @Override
    public void startAll() {
        generalWebserver.serverCreated().start();
        for (WebServerResult server : additionalWebservers) {
            server.serverCreated().start();
        }
    }

    @Override
    public void stopAll() {
        additionalWebservers.parallelStream()
                .forEach(server -> server.serverCreated().stop());
        generalWebserver.serverCreated().stop();
        if (clientStateSweepExecutor != null) {
            clientStateSweepExecutor.shutdownNow();
        }
    }

    @Override
    public Set<Integer> buildGeneralWebServer() {
        // Collect all ports registered by plugins; build a single WebServer with named sockets for extra ports.
        final LinkedHashSet<Integer> allPorts = new LinkedHashSet<>();
        allPorts.add(serverConfig.port());
        allPorts.addAll(grpcBuilders.keySet());
        allPorts.addAll(httpBuilders.keySet());
        generalWebserver = buildWebServer(allPorts, httpBuilders);
        return allPorts;
    }

    /// Builds a single [io.helidon.webserver.WebServer].
    /// The first port in the set becomes the default socket; remaining ports
    /// are registered as named sockets (`"port-<portNumber>"`) so that all
    /// listeners share the same server process. The set must be non-empty.
    ///
    /// @param ports all ports to listen on; first element is the default socket
    /// @param httpBuilders per-port HTTP routing builders
    /// @return a fully configured but not yet started [io.helidon.webserver.WebServer]
    protected WebServerResult buildWebServer(Set<Integer> ports, Map<Integer, HttpRouting.Builder> httpBuilders) {
        final PbjConfig pbjConfig = PbjConfig.builder()
                .name(PBJ_PROTOCOL_PROVIDER_CONFIG_NAME)
                .maxMessageSizeBytes(serverConfig.maxMessageSizeBytes())
                .build();
        return buildWebServer(
                ports, httpBuilders, http2Config, socketOptions, newValuesFromConfig(serverConfig), pbjConfig);
    }

    protected WebServerResult buildWebServer(
            Set<Integer> ports,
            Map<Integer, HttpRouting.Builder> httpBuilders,
            Http2Config http2Config,
            SocketOptions socketOptions,
            CommonSocketValues socketCommon,
            PbjConfig pbjConfig) {
        // Override the default message size in PBJ
        final var grpcBuilders = grpcRoutingBuilders();
        final Iterator<Integer> portIterator = ports.iterator();
        final int primaryPort = portIterator.next();
        final WebServerConfig.Builder wsBuilder = WebServerConfig.builder().port(primaryPort);
        configureSocket(
                wsBuilder,
                primaryPort,
                http2Config,
                pbjConfig,
                socketOptions,
                socketCommon,
                grpcBuilders,
                httpBuilders);
        while (portIterator.hasNext()) {
            final int port = portIterator.next();
            final ListenerConfig.Builder socketBuilder =
                    ListenerConfig.builder().port(port);
            configureListenerSocket(
                    socketBuilder,
                    port,
                    http2Config,
                    pbjConfig,
                    socketOptions,
                    socketCommon,
                    grpcBuilders,
                    httpBuilders);
            wsBuilder.putSocket("port-" + port, socketBuilder.build());
        }
        return new WebServerResult(wsBuilder.build(), ports);
    }

    private CommonSocketValues newValuesFromConfig(final ServerConfig cfg) {
        return new CommonSocketValues(
                cfg.backlogSize(),
                cfg.writeQueueLength(),
                cfg.maxTcpConnections(),
                cfg.idleConnectionPeriodMinutes(),
                cfg.idleConnectionTimeoutMinutes());
    }

    private void configureSocket(
            WebServerConfig.Builder builder,
            int port,
            Http2Config http2Config,
            PbjConfig pbjConfig,
            SocketOptions socketOptions,
            CommonSocketValues socketValues,
            Map<Integer, PbjRouting.Builder> grpcBuilders,
            Map<Integer, HttpRouting.Builder> httpBuilders) {
        builder.addProtocol(http2Config);
        builder.addProtocol(pbjConfig);
        builder.connectionOptions(socketOptions);
        applyListenerReceiveBuffer(builder, socketOptions);
        builder.backlog(socketValues.backlogSize());
        builder.writeQueueLength(socketValues.writeQueueLength());
        builder.maxTcpConnections(socketValues.maxTcpConnections());
        builder.idleConnectionPeriod(Duration.ofMinutes(socketValues.idleConnectionPeriodMinutes()));
        builder.idleConnectionTimeout(Duration.ofMinutes(socketValues.idleConnectionTimeoutMinutes()));
        final HttpRouting.Builder http = httpBuilders.get(port);
        if (http != null) builder.addRouting(http);
        final PbjRouting.Builder grpc = grpcBuilders.get(port);
        if (grpc != null) builder.addRouting(grpc);
    }

    private void configureListenerSocket(
            ListenerConfig.Builder builder,
            int port,
            Http2Config http2Config,
            PbjConfig pbjConfig,
            SocketOptions socketOptions,
            CommonSocketValues socketValues,
            Map<Integer, PbjRouting.Builder> grpcBuilders,
            Map<Integer, HttpRouting.Builder> httpBuilders) {
        builder.addProtocol(http2Config);
        builder.addProtocol(pbjConfig);
        builder.connectionOptions(socketOptions);
        applyListenerReceiveBuffer(builder, socketOptions);
        builder.backlog(socketValues.backlogSize());
        builder.writeQueueLength(socketValues.writeQueueLength());
        builder.maxTcpConnections(socketValues.maxTcpConnections());
        builder.idleConnectionPeriod(Duration.ofMinutes(socketValues.idleConnectionPeriodMinutes()));
        builder.idleConnectionTimeout(Duration.ofMinutes(socketValues.idleConnectionTimeoutMinutes()));
        final HttpRouting.Builder http = httpBuilders.get(port);
        if (http != null) builder.addRouting(http);
        final PbjRouting.Builder grpc = grpcBuilders.get(port);
        if (grpc != null) builder.addRouting(grpc);
    }

    /// Sets `SO_RCVBUF` on the listening socket so accepted connections inherit it at the TCP handshake. When no
    /// receive size is configured, nothing is set and the kernel autotunes the buffer.
    ///
    /// Helidon applies `connectionOptions` after `accept()`. Linux fixes the receive-window clamp at the handshake
    /// from the listener's buffer, and a later `SO_RCVBUF` also turns off receive-buffer autotuning, so with a
    /// configured size but without this every connection's window stays at about 32 KB (half the 64 KB kernel
    /// default). That caps a publisher stream at 32 KB per round trip, about 51 Mbit/s at a 5 ms RTT, which is all
    /// that 10k TPS leaves room for.
    ///
    /// `SO_SNDBUF` is not set here: `ServerSocketChannel` rejects it (`UnsupportedOperationException`), and the
    /// send buffer has no handshake-time effect, so applying it after `accept()` via `connectionOptions` suffices.
    ///
    /// @param builder the listener (or default web server listener) being configured
    /// @param socketOptions the connection socket options holding the configured receive buffer size
    static void applyListenerReceiveBuffer(
            @NonNull final ListenerConfig.BuilderBase<?, ?> builder, @NonNull final SocketOptions socketOptions) {
        final Optional<Integer> receiveBufferSize = socketOptions.socketReceiveBufferSize();
        if (receiveBufferSize.isPresent()) {
            builder.putListenerSocketOption(StandardSocketOptions.SO_RCVBUF, receiveBufferSize.get());
        }
    }
}
