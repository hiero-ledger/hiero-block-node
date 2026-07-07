// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.health;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.pbj.runtime.grpc.ServiceInterface;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.WebServerConfig;
import io.helidon.webserver.http.HttpRouting;
import io.helidon.webserver.http.HttpService;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.plugintest.NoBlocksHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.spi.ServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Integration test that runs the real [HealthServicePlugin] handlers behind a real Helidon
/// [WebServer] (no mocked response) and asserts, at the socket level, that every health response
/// carries the `Connection: close` header on the wire.
///
/// Why this is the right assertion: in Helidon 4.5 the server does not proactively close the TCP
/// socket after a fixed-length response; it writes the `Connection: close` header and relies on the
/// client to close (which the Kubernetes probe client does). That header is therefore our contract
/// — it is what makes probe clients release their connections instead of leaving them idle to
/// accumulate toward `server.maxTcpConnections`. If the header regresses, the client keeps the
/// connection alive and the leak returns; this test fails in that case.
///
/// The request deliberately asks for `Connection: keep-alive` to prove the server overrides it.
/// The response is read by `Content-Length` (never waiting on EOF), so the test is deterministic.
///
/// It uses [PluginTestBase] (real
/// [org.hiero.block.node.app.fixtures.plugintest.TestHealthFacility] and configuration) and captures
/// the [HttpService] the plugin registers by implementing [ServiceBuilder], then mounts it on the
/// live server.
class HealthConnectionCloseTest extends PluginTestBase<HealthServicePlugin, BlockingExecutor, ScheduledExecutorService>
        implements ServiceBuilder {

    private static final int SOCKET_TIMEOUT_MILLIS = 5_000;

    /// Captures the HTTP services the plugin registers during {@code init}, keyed by base path.
    private final Map<String, HttpService> registeredServices = new HashMap<>();

    private WebServer webServer;

    HealthConnectionCloseTest() {
        super(
                new BlockingExecutor(new LinkedBlockingQueue<>()),
                new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
    }

    /// Because this test implements {@link ServiceBuilder}, {@code PluginTestBase} passes it to
    /// {@code plugin.init(...)}, so the plugin registers its real routing here.
    @Override
    public void registerHttpService(final String path, @Nullable final Integer port, final HttpService... service) {
        for (final HttpService httpService : service) {
            registeredServices.put(path, httpService);
        }
    }

    @Override
    public void registerGrpcService(@NonNull final ServiceInterface service, @Nullable final Integer port) {
        // health plugin registers no gRPC services
    }

    @BeforeEach
    void startServer() {
        start(new HealthServicePlugin(), new NoBlocksHistoricalBlockFacility());
        final HttpRouting.Builder routing = HttpRouting.builder();
        registeredServices.forEach(routing::register);
        webServer = WebServerConfig.builder().port(0).addRouting(routing).build();
        webServer.start();
    }

    @AfterEach
    void stopServer() {
        if (webServer != null) {
            webServer.stop();
        }
    }

    @Test
    @DisplayName("livez sends Connection: close on the wire")
    void livezSendsConnectionClose() throws IOException {
        final HttpResponse response = fetch("/healthz/livez");
        assertEquals(200, response.statusCode(), response::head);
        assertConnectionClose(response);
        assertEquals("OK", response.body());
    }

    @Test
    @DisplayName("readyz sends Connection: close on the wire")
    void readyzSendsConnectionClose() throws IOException {
        final HttpResponse response = fetch("/healthz/readyz");
        assertEquals(200, response.statusCode(), response::head);
        assertConnectionClose(response);
        assertEquals("OK", response.body());
    }

    @Test
    @DisplayName("statusz responses also send Connection: close on the wire")
    void statuszSendsConnectionClose() throws IOException {
        // The statusz handler sets Connection: close on every branch (JSON 200 and the 404/500
        // fallbacks); JSON 200 dispatch is covered by HealthServiceTest. Here we only assert the
        // header contract survives to a real socket through the statusz handler.
        final HttpResponse response = fetch("/statusz/inbound");
        assertConnectionClose(response);
    }

    private static void assertConnectionClose(final HttpResponse response) {
        assertTrue(
                response.head().toLowerCase().contains("connection: close"),
                () -> "response is missing 'Connection: close' header:\n" + response.head());
    }

    /// Sends an HTTP/1.1 GET that asks to keep the connection alive, then reads the status line and
    /// headers, and the body by `Content-Length`. Never waits for EOF, so a kept-alive connection
    /// cannot hang the test.
    @NonNull
    private HttpResponse fetch(@NonNull final String path) throws IOException {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", webServer.port()), SOCKET_TIMEOUT_MILLIS);
            socket.setSoTimeout(SOCKET_TIMEOUT_MILLIS);

            final OutputStream out = socket.getOutputStream();
            out.write(("GET " + path + " HTTP/1.1\r\n" + "Host: 127.0.0.1\r\n" + "Connection: keep-alive\r\n" + "\r\n")
                    .getBytes(US_ASCII));
            out.flush();

            final InputStream in = socket.getInputStream();
            final String head = readHead(in);
            final String body = readBody(in, contentLength(head));
            return new HttpResponse(head, body);
        }
    }

    /// Reads the status line and header block up to (and excluding) the terminating blank line.
    @NonNull
    private static String readHead(@NonNull final InputStream in) throws IOException {
        final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            buffer.write(b);
            final byte[] bytes = buffer.toByteArray();
            final int length = bytes.length;
            if (length >= 4
                    && bytes[length - 4] == '\r'
                    && bytes[length - 3] == '\n'
                    && bytes[length - 2] == '\r'
                    && bytes[length - 1] == '\n') {
                return new String(bytes, 0, length - 4, US_ASCII);
            }
        }
        return buffer.toString(US_ASCII);
    }

    @NonNull
    private static String readBody(@NonNull final InputStream in, final int contentLength) throws IOException {
        final byte[] body = new byte[contentLength];
        int offset = 0;
        while (offset < contentLength) {
            final int read = in.read(body, offset, contentLength - offset);
            if (read == -1) {
                break;
            }
            offset += read;
        }
        return new String(body, 0, offset, UTF_8);
    }

    private static int contentLength(@NonNull final String head) {
        for (final String line : head.split("\r\n")) {
            if (line.toLowerCase().startsWith("content-length:")) {
                return Integer.parseInt(line.substring(line.indexOf(':') + 1).trim());
            }
        }
        return 0;
    }

    /// Parsed HTTP response: the raw head (status line + headers) and the decoded body.
    private record HttpResponse(
            @NonNull String head, @NonNull String body) {
        int statusCode() {
            // status line looks like: HTTP/1.1 200 OK
            final String[] parts = head.split("\r\n", 2)[0].split(" ");
            return Integer.parseInt(parts[1]);
        }
    }
}
