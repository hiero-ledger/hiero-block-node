// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

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
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import org.hiero.block.node.app.config.ServerConfig;
import org.hiero.block.node.spi.ServiceBuilder;

/// Default implementation of [ServiceBuilder]. That builds HTTP and PBJ GRPC services.
///
/// Services are bucketed by port number. [BlockNodeApp] creates one
/// [io.helidon.webserver.WebServer] per distinct port found in the maps, so registering all
/// services on the same port results in a single listener with all routes merged.
///
/// A `null` port in any registration call resolves to the default port supplied at
/// construction time (typically `server.port`).
public class ServiceBuilderImpl implements ServiceBuilder {
    /** Per-port HTTP routing builders. */
    private final Map<Integer, HttpRouting.Builder> httpBuilders = new HashMap<>();
    /** Per-port PBJ gRPC routing builders. */
    private final Map<Integer, PbjRouting.Builder> grpcBuilders = new HashMap<>();

    private final ServerConfig serverConfig;
    private final Http2Config http2Config;
    private final SocketOptions socketOptions;
    private WebServerResult generalWebserver;
    private final LinkedHashSet<WebServerResult> additionalWebservers;

    public ServiceBuilderImpl(
            final ServerConfig serverConfig, final Http2Config http2Config, final SocketOptions socketOptions) {
        this.serverConfig = serverConfig;
        this.http2Config = http2Config;
        this.socketOptions = socketOptions;
        additionalWebservers = new LinkedHashSet<>();
    }

    /// {@inheritDoc}
    @Override
    public void registerHttpService(@NonNull String path, @Nullable Integer port, @NonNull HttpService... service) {
        httpBuilders.computeIfAbsent(resolve(port), k -> HttpRouting.builder()).register(path, service);
    }

    /// {@inheritDoc}
    @Override
    public void registerGrpcService(@Nullable Integer port, @NonNull ServiceInterface service) {
        grpcBuilders.computeIfAbsent(resolve(port), k -> PbjRouting.builder()).service(service);
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
}
