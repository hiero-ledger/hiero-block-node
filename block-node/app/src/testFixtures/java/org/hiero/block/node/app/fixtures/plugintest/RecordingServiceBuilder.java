// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.plugintest;

import com.hedera.pbj.runtime.grpc.ServiceInterface;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.helidon.common.socket.SocketOptions;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http2.Http2Config;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.hiero.block.node.spi.ServiceBuilder;

/// A [ServiceBuilder] test fixture that records every call and every input instead of creating
/// real Helidon web servers. It never opens a socket; it simply captures what a plugin registered so
/// that test code can assert on it via the public accessor methods.
///
/// This fixture is intentionally plugin-agnostic and is meant to be reused across the tests of any
/// plugin that registers services through a [ServiceBuilder].
///
/// ## Server model
///
///   - There is a single **"general" web server**, numbered **#1**. It is created by
///     [#buildGeneralWebServer()] and owns every [#registerHttpService] /
///     [#registerGrpcService] registration. Its port set is [#DEFAULT_GENERAL_PORT]
///     (or the value supplied to [#RecordingServiceBuilder(int)]) unioned with the resolved
///     port of every such registration (a `null` port resolves to the default port).
///   - Every [#registerHttpNewServer] call (either overload) creates one **additional**
///     web server, numbered **#2, #3, …** in the order the calls are made (across both
///     overloads). Its port set is the key set of the supplied services map.
///
/// The default port is [#DEFAULT_GENERAL_PORT] = `131072`, which is deliberately larger
/// than the maximum valid TCP port (65535). Because this fixture never binds a socket, using an
/// out-of-range sentinel keeps recorded ports from ever being confused with a real one.
public final class RecordingServiceBuilder implements ServiceBuilder {

    /// The default "general" port. Deliberately out of the valid TCP range (> 65535) so it can never
    /// collide with a real port; this fixture never opens a socket.
    public static final int DEFAULT_GENERAL_PORT = 131_072;

    /// A single recorded {@link #registerHttpService} invocation.
    ///
    /// @param path the path exactly as supplied
    /// @param port the port exactly as supplied (may be {@code null}, meaning "use the default port")
    /// @param services an immutable copy of the services supplied
    public record HttpServiceRegistration(
            @NonNull String path,
            @Nullable Integer port,
            @NonNull List<HttpService> services) {}

    /// A single recorded {@link #registerGrpcService} invocation.
    ///
    /// @param port the port exactly as supplied (may be {@code null}, meaning "use the default port")
    /// @param service the gRPC service supplied
    public record GrpcServiceRegistration(
            @Nullable Integer port, @NonNull ServiceInterface service) {}

    /// A single recorded {@link #registerHttpNewServer} invocation (either overload).
    ///
    /// @param serverNumber the number assigned to this additional server (>= 2)
    /// @param services a defensive copy of the services map supplied
    /// @param http2Config the HTTP/2 config supplied, or {@code null} for the two-argument overload
    /// @param socketOptions the socket options supplied, or {@code null} for the two-argument overload
    /// @param commonSocketValues the common socket values supplied
    /// @param portsEnabled an immutable copy of the ports this server would listen on (the map key set)
    public record NewServerRegistration(
            int serverNumber,
            @NonNull TreeMap<Integer, ServiceWithPath[]> services,
            @Nullable Http2Config http2Config,
            @Nullable SocketOptions socketOptions,
            @Nullable CommonSocketValues commonSocketValues,
            @NonNull Set<Integer> portsEnabled) {}

    /// A web server (general or additional) together with the ports it would have been started or
    /// stopped on. Used by {@link #startedServers()} and {@link #stoppedServers()}.
    ///
    /// @param serverNumber the server number (1 == the general server)
    /// @param ports an immutable copy of that server's ports
    public record ServerPorts(int serverNumber, @NonNull Set<Integer> ports) {}

    /** The port a {@code null} port resolves to, and the general server's base port. */
    private final int defaultPort;

    /** Every {@link #registerHttpService} invocation, in call order. */
    private final List<HttpServiceRegistration> httpServiceRegistrations = new ArrayList<>();
    /** Every {@link #registerGrpcService} invocation, in call order. */
    private final List<GrpcServiceRegistration> grpcServiceRegistrations = new ArrayList<>();
    /** Every two-argument {@link #registerHttpNewServer(TreeMap, CommonSocketValues)} invocation, in call order. */
    private final List<NewServerRegistration> newServerRegistrations = new ArrayList<>();
    /** Every four-argument {@link #registerHttpNewServer(TreeMap, Http2Config, SocketOptions, CommonSocketValues)} invocation. */
    private final List<NewServerRegistration> newServerWithOptionsRegistrations = new ArrayList<>();

    /** Servers recorded by each {@link #startAll()} call, appended in start order. */
    private final List<ServerPorts> startedServers = new ArrayList<>();
    /** Servers recorded by each {@link #stopAll()} call, appended in stop order. */
    private final List<ServerPorts> stoppedServers = new ArrayList<>();

    /** The next number to hand out to an additional server (the general server is always #1). */
    private int nextAdditionalServerNumber = 2;
    /** The general server's ports, or {@code null} until {@link #buildGeneralWebServer()} is called. */
    private Set<Integer> generalServerPorts = null;

    private int buildGeneralWebServerCallCount = 0;
    private int startAllCallCount = 0;
    private int stopAllCallCount = 0;

    /// Creates a fixture whose default (general) port is {@link #DEFAULT_GENERAL_PORT}.
    public RecordingServiceBuilder() {
        this(DEFAULT_GENERAL_PORT);
    }

    /// Creates a fixture with an explicit default (general) port that {@code null} ports resolve to.
    ///
    /// @param defaultPort the port a {@code null} port resolves to, and the general server's base port
    public RecordingServiceBuilder(final int defaultPort) {
        this.defaultPort = defaultPort;
    }

    // -----------------------------------------------------------------------------------------------
    // ServiceBuilder — recording implementations
    // -----------------------------------------------------------------------------------------------

    /// {@inheritDoc}
    /// Records the invocation; does not register anything with a real server.
    @Override
    public void registerHttpService(
            @NonNull final String path, @Nullable final Integer port, @NonNull final HttpService... service) {
        httpServiceRegistrations.add(new HttpServiceRegistration(path, port, List.of(service)));
    }

    /// {@inheritDoc}
    /// Records the invocation; does not register anything with a real server.
    @Override
    public void registerGrpcService(@Nullable final Integer port, @NonNull final ServiceInterface service) {
        grpcServiceRegistrations.add(new GrpcServiceRegistration(port, service));
    }

    /// {@inheritDoc}
    /// Records the invocation as a new additional server and returns a {@link WebServerResult} whose
    /// ports are the supplied map's key set and whose {@code WebServer} is {@code null}.
    @Override
    public WebServerResult registerHttpNewServer(
            @NonNull final TreeMap<Integer, ServiceWithPath[]> services, final CommonSocketValues commonSocketValues) {
        return recordNewServer(services, null, null, commonSocketValues, newServerRegistrations);
    }

    /// {@inheritDoc}
    /// Records the invocation (including the HTTP/2 config and socket options) as a new additional
    /// server and returns a {@link WebServerResult} whose ports are the supplied map's key set and
    /// whose {@code WebServer} is {@code null}.
    @Override
    public WebServerResult registerHttpNewServer(
            @NonNull final TreeMap<Integer, ServiceWithPath[]> services,
            final Http2Config http2Config,
            final SocketOptions socketOptions,
            final CommonSocketValues commonSocketValues) {
        return recordNewServer(
                services, http2Config, socketOptions, commonSocketValues, newServerWithOptionsRegistrations);
    }

    /// {@inheritDoc}
    /// Computes and records the general server's ports: the default port unioned with the resolved
    /// port of every recorded HTTP/gRPC registration.
    @Override
    public Set<Integer> buildGeneralWebServer() {
        final LinkedHashSet<Integer> ports = new LinkedHashSet<>();
        ports.add(defaultPort);
        for (final HttpServiceRegistration registration : httpServiceRegistrations) {
            ports.add(resolve(registration.port()));
        }
        for (final GrpcServiceRegistration registration : grpcServiceRegistrations) {
            ports.add(resolve(registration.port()));
        }
        generalServerPorts = immutableCopy(ports);
        buildGeneralWebServerCallCount++;
        return generalServerPorts;
    }

    /// {@inheritDoc}
    /// Records the servers (and their ports) that would have been started: the general server (#1)
    /// first, if it was built, followed by the additional servers in ascending server-number order.
    @Override
    public void startAll() {
        final List<ServerPorts> ordered = new ArrayList<>();
        if (generalServerPorts != null) {
            ordered.add(new ServerPorts(1, generalServerPorts));
        }
        ordered.addAll(additionalServersOrdered());
        startedServers.addAll(ordered);
        startAllCallCount++;
    }

    /// {@inheritDoc}
    /// Records the servers (and their ports) that would have been stopped, mirroring production stop
    /// order: the additional servers (ascending server-number order) first, then the general server
    /// (#1) last, if it was built.
    @Override
    public void stopAll() {
        final List<ServerPorts> ordered = new ArrayList<>(additionalServersOrdered());
        if (generalServerPorts != null) {
            ordered.add(new ServerPorts(1, generalServerPorts));
        }
        stoppedServers.addAll(ordered);
        stopAllCallCount++;
    }

    // -----------------------------------------------------------------------------------------------
    // Accessors — every recorded value is exposed here for test code
    // -----------------------------------------------------------------------------------------------

    /// @return the default (general) port that {@code null} ports resolve to
    public int defaultPort() {
        return defaultPort;
    }

    /// @return an immutable snapshot of every recorded {@link #registerHttpService} invocation
    @NonNull
    public List<HttpServiceRegistration> httpServiceRegistrations() {
        return List.copyOf(httpServiceRegistrations);
    }

    /// @return an immutable snapshot of every recorded {@link #registerGrpcService} invocation
    @NonNull
    public List<GrpcServiceRegistration> grpcServiceRegistrations() {
        return List.copyOf(grpcServiceRegistrations);
    }

    /// @return an immutable snapshot of every recorded two-argument
    ///     {@link #registerHttpNewServer(TreeMap, CommonSocketValues)} invocation
    @NonNull
    public List<NewServerRegistration> newServerRegistrations() {
        return List.copyOf(newServerRegistrations);
    }

    /// @return an immutable snapshot of every recorded four-argument
    ///     {@link #registerHttpNewServer(TreeMap, Http2Config, SocketOptions, CommonSocketValues)} invocation
    @NonNull
    public List<NewServerRegistration> newServerWithOptionsRegistrations() {
        return List.copyOf(newServerWithOptionsRegistrations);
    }

    /// @return every additional-server registration from both {@link #registerHttpNewServer} overloads,
    ///     merged and sorted by server number (i.e. creation order)
    @NonNull
    public List<NewServerRegistration> allNewServerRegistrations() {
        return Stream.concat(newServerRegistrations.stream(), newServerWithOptionsRegistrations.stream())
                .sorted(Comparator.comparingInt(NewServerRegistration::serverNumber))
                .toList();
    }

    /// @return the general server's ports, or an empty set if {@link #buildGeneralWebServer()} has not
    ///     been called
    @NonNull
    public Set<Integer> generalServerPorts() {
        return generalServerPorts == null ? Set.of() : generalServerPorts;
    }

    /// @return {@code true} if {@link #buildGeneralWebServer()} has been called at least once
    public boolean generalServerBuilt() {
        return generalServerPorts != null;
    }

    /// @return the number of times {@link #buildGeneralWebServer()} has been called
    public int buildGeneralWebServerCallCount() {
        return buildGeneralWebServerCallCount;
    }

    /// @return an immutable snapshot of the servers recorded across all {@link #startAll()} calls, in
    ///     start order (general server #1 first, then additional servers ascending)
    @NonNull
    public List<ServerPorts> startedServers() {
        return List.copyOf(startedServers);
    }

    /// @return an immutable snapshot of the servers recorded across all {@link #stopAll()} calls, in
    ///     stop order (additional servers ascending, then general server #1 last)
    @NonNull
    public List<ServerPorts> stoppedServers() {
        return List.copyOf(stoppedServers);
    }

    /// @return the number of times {@link #startAll()} has been called
    public int startAllCallCount() {
        return startAllCallCount;
    }

    /// @return the number of times {@link #stopAll()} has been called
    public int stopAllCallCount() {
        return stopAllCallCount;
    }

    // -----------------------------------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------------------------------

    /// Records a new additional server for the given overload and returns its {@link WebServerResult}
    /// (with a {@code null} {@code WebServer} and the map's key set as the enabled ports).
    private WebServerResult recordNewServer(
            @NonNull final TreeMap<Integer, ServiceWithPath[]> services,
            @Nullable final Http2Config http2Config,
            @Nullable final SocketOptions socketOptions,
            @Nullable final CommonSocketValues commonSocketValues,
            @NonNull final List<NewServerRegistration> target) {
        final Set<Integer> portsEnabled = immutableCopy(services.keySet());
        target.add(new NewServerRegistration(
                nextAdditionalServerNumber++,
                new TreeMap<>(services),
                http2Config,
                socketOptions,
                commonSocketValues,
                portsEnabled));
        return new WebServerResult(null, portsEnabled);
    }

    /// The additional servers (both overloads) as {@link ServerPorts}, ascending by server number.
    private List<ServerPorts> additionalServersOrdered() {
        return allNewServerRegistrations().stream()
                .map(registration -> new ServerPorts(registration.serverNumber(), registration.portsEnabled()))
                .toList();
    }

    /// Resolves a possibly-{@code null} port to the default port.
    private int resolve(@Nullable final Integer port) {
        return port != null ? port : defaultPort;
    }

    /// Returns an unmodifiable, insertion-ordered copy of the given set.
    private static Set<Integer> immutableCopy(@NonNull final Set<Integer> source) {
        return Collections.unmodifiableSet(new LinkedHashSet<>(source));
    }
}
