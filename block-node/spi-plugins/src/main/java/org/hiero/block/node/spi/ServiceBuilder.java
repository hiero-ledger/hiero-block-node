// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi;

import com.hedera.pbj.runtime.grpc.ServiceInterface;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.helidon.common.socket.SocketOptions;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http2.Http2Config;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.hiero.block.node.spi.throttle.BlockReadBulkhead;
import org.hiero.block.node.spi.throttle.ContentAwareWeigher;
import org.hiero.block.node.spi.throttle.PerClientThrottleSettings;
import org.hiero.block.node.spi.throttle.WeightClass;

/// ServiceBuilder is an interface that defines the contract for registering HTTP and gRPC services
/// with the web server during initialization.
///
/// Each registration method accepts an optional port number. When `port` is `null`
/// the runtime binds the service to the default server port (`server.port`). Plugins that
/// need their own dedicated port obtain it from their own [BlockNodeContext] configuration
/// and pass it explicitly; plugins that are happy sharing the default port simply pass `null`.
public interface ServiceBuilder {
    /// Constant mapped to PbjProtocolProvider.CONFIG\_NAME in the PBJ Helidon Plugin
    String PBJ_PROTOCOL_PROVIDER_CONFIG_NAME = "pbj";

    /// A path and one or more HTTP services.
    /// This is used as input to creating a specialized HTTP web server.
    ///
    /// @param path A single URL path.
    /// @param services One or more HTTP services.
    public static record ServiceWithPath(String path, HttpService... services) {}

    /// The result for a requested webserver creation.
    ///
    /// @param serverCreated A webserver that was newly created.
    /// @param portsEnabled The ports routed to this server.
    public static record WebServerResult(WebServer serverCreated, Set<Integer> portsEnabled) {}

    /// The socket values shared between socket options and http 2 config
    /// @param backlogSize the backlog entries reserved for listener sockets.
    /// @param writeQueueLength the write queue length for reserved for
    ///     response sockets.
    /// @param maxTcpConnections the maximum active TCP connections to support
    ///     on listener sockets
    /// @param idleConnectionPeriodMinutes the check interval for idle
    ///     connections on listener sockets
    /// @param idleConnectionTimeoutMinutes the maximum time to allow a
    ///     connection on a listener socket to remain idle before it is closed.
    ///     Only applies to HTTP/1 connections.
    public static record CommonSocketValues(
            int backlogSize,
            int writeQueueLength,
            int maxTcpConnections,
            int idleConnectionPeriodMinutes,
            int idleConnectionTimeoutMinutes) {}

    /// Registers an HTTP service on the given port, or on the default port
    /// if `port` is `null`. This service is added to the "General" webserver.
    ///
    /// @param path the path for the HTTP service
    /// @param port the port number to bind this service to, or `null` to use
    ///     the default port
    /// @param service the HTTP services to register at that path, must not
    ///     be empty.
    void registerHttpService(@NonNull String path, @Nullable Integer port, @NonNull HttpService... service);

    /// Registers a gRPC service on the given port, or on the default port
    /// if `port` is `null`. This service is added to the "General" webserver.
    ///
    /// @param port the port number to bind this service to, or `null` to
    /// use the default port
    /// @param service the gRPC service to register
    void registerGrpcService(@Nullable Integer port, @NonNull ServiceInterface service);

    /// Registers a gRPC service on the given port, or on the default port if `port` is `null`,
    /// with per-client rate and concurrency admission control applied.
    ///
    /// The registration point merges `perClientSettings` with the node-wide concurrency ceiling
    /// for this service (resolved internally) into a full throttle policy before wrapping the
    /// service. See `docs/design/apis/api-throttling.md` for the full design.
    ///
    /// @param port the port number to bind this service to, or `null` to use the default port
    /// @param service the gRPC service to register and protect
    /// @param perClientSettings this service's own per-client rate/concurrency settings
    void registerGrpcService(
            @Nullable Integer port,
            @NonNull ServiceInterface service,
            @NonNull PerClientThrottleSettings perClientSettings);

    /// Registers a gRPC service on the given port, or on the default port if `port` is `null`,
    /// with per-client rate/concurrency admission control that varies by request content.
    ///
    /// Use this overload instead of the single-policy one when a service's methods have more than
    /// one cost tier (e.g. `getBlock` requests for a live block versus a historical/archived one).
    /// `weigher` classifies each call once its request content is available (not at registration
    /// time, and not synchronously inside the service's `open()` — see
    /// `org.hiero.block.node.spi.throttle.WeightedThrottledServiceInterface` for why), and the
    /// matching entry in `perClientSettingsByWeight` governs that call. The map must contain an
    /// entry for [WeightClass#STANDARD], used as the fallback if the weigher ever returns a class
    /// with no configured entry.
    ///
    /// @param port the port number to bind this service to, or `null` to use the default port
    /// @param service the gRPC service to register and protect
    /// @param perClientSettingsByWeight this service's per-client rate/concurrency settings, one
    ///     entry per weight class its weigher can classify a request into
    /// @param weigher classifies each call's request content into a weight class
    void registerGrpcService(
            @Nullable Integer port,
            @NonNull ServiceInterface service,
            @NonNull Map<WeightClass, PerClientThrottleSettings> perClientSettingsByWeight,
            @NonNull ContentAwareWeigher weigher);

    /// The single, shared [BlockReadBulkhead] protecting block storage from combined read load
    /// across every API that reads from it (Component B in `docs/design/apis/api-throttling.md`).
    /// The same instance is returned on every call, so every plugin that reads block storage on
    /// behalf of a client request (currently `getBlock` and the subscriber's historical catch-up
    /// path) shares one bounded pool rather than each having its own.
    ///
    /// @return the node's single shared block-read bulkhead
    @NonNull
    BlockReadBulkhead blockReadBulkhead();

    /// Registers a new webserver configured with one or more HTTP services
    /// attached to a set of ports.
    ///
    /// @param services A map from port number to a combination of path and
    ///     one or more HTTP services. This webserver is provided for reference,
    ///     but must not be started or stopped independently. This server will
    ///     be started and stopped with all other servers via the [#startAll]
    ///     and [#stopAll] methods.
    /// @param commonSocketValues The unique common socket options to set for
    ///     the new server.
    /// @return a result containing a webserver and a set of all configured ports.
    WebServerResult registerHttpNewServer(
            final TreeMap<Integer, ServiceWithPath[]> services, final CommonSocketValues commonSocketValues);

    /// Registers a new webserver configured with one or more HTTP services
    /// attached to a set of ports.
    ///
    /// @param services A map from port number to a combination of path and
    ///     one or more HTTP services. This webserver is provided for reference,
    ///     but must not be started or stopped independently. This server will
    ///     be started and stopped with all other servers via the [#startAll]
    ///     and [#stopAll] methods.
    /// @param http2Config The unique HTTP/2 configuration for the new server.
    /// @param socketOptions The unique socket options for the new server
    /// @param commonSocketValues The unique common socket options to set for
    ///     the new server.
    /// @return a result containing a webserver and a set of all configured ports.
    WebServerResult registerHttpNewServer(
            final TreeMap<Integer, ServiceWithPath[]> services,
            final Http2Config http2Config,
            final SocketOptions socketOptions,
            final CommonSocketValues commonSocketValues);

    /// Builds a single [WebServer] for "general" use.
    /// The default socket port is implementation specific; remaining ports
    /// are derived from grpcBuilders and httpBuilders, and are registered as
    /// named sockets (`"port-<portNumber>"`) so that all listeners share the
    /// same server instance.
    ///
    /// @return a Set of Integers, one for each port assigned to the
    ///     general web server.
    Set<Integer> buildGeneralWebServer();

    /// Start all configured web servers, beginning with the "General" server.
    void startAll();

    /// Stop all configured web servers in parallel.
    void stopAll();
}
