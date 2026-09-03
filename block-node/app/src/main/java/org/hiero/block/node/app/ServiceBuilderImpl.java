// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import static java.lang.System.Logger.Level.INFO;
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
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.hiero.block.node.app.config.BlockReadBulkheadConfig;
import org.hiero.block.node.app.config.GlobalThrottleConfig;
import org.hiero.block.node.app.config.ServerConfig;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.threading.ThreadPoolManager;
import org.hiero.block.node.spi.throttle.BlockReadBulkhead;
import org.hiero.block.node.spi.throttle.ContentAwareWeigher;
import org.hiero.block.node.spi.throttle.PerClientThrottleSettings;
import org.hiero.block.node.spi.throttle.RemoteAddressKeyExtractor;
import org.hiero.block.node.spi.throttle.StaleClientSweepable;
import org.hiero.block.node.spi.throttle.ThrottleExempt;
import org.hiero.block.node.spi.throttle.ThrottlePolicy;
import org.hiero.block.node.spi.throttle.ThrottleSpec;
import org.hiero.block.node.spi.throttle.ThrottledServiceInterface;
import org.hiero.block.node.spi.throttle.WeightClass;
import org.hiero.block.node.spi.throttle.WeightedThrottledServiceInterface;
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

    /** Every throttled service this instance has created, for the periodic stale-client sweep. */
    private final List<StaleClientSweepable> throttledServices = new ArrayList<>();
    /** Lazily created on the first throttled registration; runs the stale-client-state sweep. */
    private ScheduledExecutorService clientStateSweepExecutor;
    /** The single, shared block-read bulkhead every plugin's [#blockReadBulkhead] call returns. */
    private final BlockReadBulkhead blockReadBulkhead;

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
    /// Every registration is logged once, at startup, as one of throttled ([ThrottleSpec]), exempt
    /// ([ThrottleExempt]), or neither — the last case logs a warning, since an admission-control gap
    /// is far more likely to be an oversight than a deliberate choice, and nothing else here would
    /// otherwise make that omission visible.
    ///
    /// If `service` also implements [ThrottleSpec], merges its per-weight-class per-client settings
    /// with its own reported [ThrottleSpec#globalConcurrencyCeilings], and registers the resulting
    /// [ThrottledServiceInterface] or [WeightedThrottledServiceInterface] in place of the raw service.
    /// A spec with no [ThrottleSpec#weigher] gets the lighter-weight [ThrottledServiceInterface], which
    /// decides admission synchronously inside `open()` rather than deferring to `onNext()` — the same
    /// latency characteristic a single-tier service always had before this method was unified. Adding
    /// a new throttled method means the plugin implementing [ThrottleSpec] end to end, including its
    /// own [ThrottleSpec#globalConcurrencyCeilings] lookup against the centrally-owned
    /// [GlobalThrottleConfig] — nothing in this class needs to change.
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
                        WARNING,
                        "Registered gRPC service {0}: NOT throttled, and not marked exempt via ThrottleExempt — "
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
        final Map<WeightClass, PerClientThrottleSettings> perClientSettingsByWeight = spec.perClientSettingsByWeight();
        final Map<WeightClass, Integer> globalConcurrencyCeilings = spec.globalConcurrencyCeilings();
        final Optional<ContentAwareWeigher> weigher = spec.weigher();
        final ServiceInterface throttled;
        if (weigher.isPresent()) {
            final Map<WeightClass, ThrottlePolicy> policiesByWeight = new EnumMap<>(WeightClass.class);
            for (final Entry<WeightClass, PerClientThrottleSettings> entry : perClientSettingsByWeight.entrySet()) {
                final int maxConcurrentGlobal =
                        requireGlobalCeiling(service, entry.getKey(), globalConcurrencyCeilings);
                policiesByWeight.put(entry.getKey(), ThrottlePolicy.merge(entry.getValue(), maxConcurrentGlobal));
            }
            final WeightedThrottledServiceInterface weightedThrottled = new WeightedThrottledServiceInterface(
                    service,
                    policiesByWeight,
                    new RemoteAddressKeyExtractor(),
                    weigher.get(),
                    metricRegistry,
                    Duration.ofMinutes(globalThrottleConfig.clientStateTtlMinutes()));
            throttledServices.add(weightedThrottled);
            throttled = weightedThrottled;
        } else {
            final PerClientThrottleSettings perClientSettings = perClientSettingsByWeight.get(WeightClass.STANDARD);
            if (perClientSettings == null) {
                throw new IllegalArgumentException(
                        "ThrottleSpec with no weigher must supply WeightClass.STANDARD settings for "
                                + service.serviceName());
            }
            final int maxConcurrentGlobal =
                    requireGlobalCeiling(service, WeightClass.STANDARD, globalConcurrencyCeilings);
            final ThrottlePolicy policy = ThrottlePolicy.merge(perClientSettings, maxConcurrentGlobal);
            final ThrottledServiceInterface simpleThrottled = new ThrottledServiceInterface(
                    service,
                    policy,
                    new RemoteAddressKeyExtractor(),
                    metricRegistry,
                    Duration.ofMinutes(globalThrottleConfig.clientStateTtlMinutes()));
            throttledServices.add(simpleThrottled);
            throttled = simpleThrottled;
        }
        ensureClientStateSweepStarted();
        grpcBuilders.computeIfAbsent(resolve(port), k -> PbjRouting.builder()).service(throttled);
    }

    /// Looks up one weight class's node-wide ceiling from a [ThrottleSpec]'s own reported map,
    /// failing clearly if the spec's [ThrottleSpec#globalConcurrencyCeilings] doesn't cover a weight
    /// class its own [ThrottleSpec#perClientSettingsByWeight] declares — a plugin-authoring mistake,
    /// not something this class can resolve on the plugin's behalf.
    private static int requireGlobalCeiling(
            @NonNull final ServiceInterface service,
            @NonNull final WeightClass weightClass,
            @NonNull final Map<WeightClass, Integer> globalConcurrencyCeilings) {
        final Integer ceiling = globalConcurrencyCeilings.get(weightClass);
        if (ceiling == null) {
            throw new IllegalArgumentException("ThrottleSpec for " + service.serviceName()
                    + " has no globalConcurrencyCeilings entry for weight class " + weightClass);
        }
        return ceiling;
    }

    /// Starts the periodic stale-client-state sweep the first time it's needed (i.e. the first
    /// throttled service registration), rather than unconditionally in the constructor — a node
    /// with no throttled services registers nothing and spawns no sweep thread.
    private void ensureClientStateSweepStarted() {
        if (clientStateSweepExecutor != null) {
            return;
        }
        clientStateSweepExecutor = threadPoolManager.createVirtualThreadScheduledExecutor(
                1, "throttle-client-state-sweep", (thread, throwable) -> {});
        final long intervalMinutes = globalThrottleConfig.clientStateSweepIntervalMinutes();
        clientStateSweepExecutor.scheduleAtFixedRate(
                () -> {
                    final long now = System.nanoTime();
                    for (final StaleClientSweepable throttled : throttledServices) {
                        throttled.sweepStaleClients(now);
                    }
                },
                intervalMinutes,
                intervalMinutes,
                TimeUnit.MINUTES);
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

    /// Sets `SO_RCVBUF` on the listening socket so accepted connections inherit it at the TCP handshake.
    ///
    /// Helidon applies `connectionOptions` after `accept()`. Linux fixes the receive-window clamp at the handshake
    /// from the listener's buffer, and a later `SO_RCVBUF` also turns off receive-buffer autotuning, so without
    /// this every connection's window stays at about 32 KB (half the 64 KB kernel default). That caps a publisher
    /// stream at 32 KB per round trip, about 51 Mbit/s at a 5 ms RTT, which is all that 10k TPS leaves room for.
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
