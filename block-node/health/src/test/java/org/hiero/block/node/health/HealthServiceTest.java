// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.health;

import static io.helidon.http.HeaderValues.CONNECTION_CLOSE;
import static org.hiero.block.node.health.HealthServicePlugin.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.Configuration;
import io.helidon.http.HttpPrologue;
import io.helidon.http.Method;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.TreeMap;
import org.hiero.block.api.NetworkData;
import org.hiero.block.node.app.config.WebServerHttp2Config;
import org.hiero.block.node.app.fixtures.TestMetricsExporter;
import org.hiero.block.node.app.fixtures.plugintest.TestApplicationStateFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestServerRequest;
import org.hiero.block.node.app.fixtures.plugintest.TestServerResponse;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.ServiceBuilder.ServiceWithPath;
import org.hiero.block.node.spi.health.HealthFacility;
import org.hiero.metrics.core.MetricRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class HealthServiceTest {

    @Mock
    ServerRequest serverRequest;

    @Mock
    ServerResponse serverResponse;

    @Mock
    ServiceBuilder serviceBuilder;

    /** Captures metric values so tests can assert the request counter (one per test instance). */
    private final TestMetricsExporter metricsExporter = new TestMetricsExporter();

    /**
     * Stub the context configuration so {@code init} can read {@link HealthConfig} (a dedicated health
     * port), and wire a real {@link MetricRegistry} so {@code init} can register the request counter.
     */
    private void setupContextConfig(BlockNodeContext context) {
        final Configuration configuration = Mockito.mock(Configuration.class);
        // port + socket/connection settings (mirroring the HealthConfig defaults)
        Mockito.when(configuration.getConfigData(HealthConfig.class))
                .thenReturn(new HealthConfig(40983, 32_768, 32_768, 512, 512, 10, 1, 2, true));
        Mockito.when(configuration.getConfigData(WebServerHttp2Config.class))
                .thenReturn(new WebServerHttp2Config(500, 8_388_608, 8L, 10, 8_388_608, 8192L, 50, 10000));
        Mockito.when(context.configuration()).thenReturn(configuration);
        Mockito.when(context.metricRegistry())
                .thenReturn(MetricRegistry.builder()
                        .setMetricsExporter(metricsExporter)
                        .build());
    }

    /** Stubs {@code request.prologue()} to report HTTP/1.1, matching a real Kubernetes probe. */
    private static void stubHttp1Request(ServerRequest request) {
        Mockito.when(request.prologue())
                .thenReturn(HttpPrologue.create("HTTP/1.1", "HTTP", "1.1", Method.GET, "/", false));
    }

    @Test
    public void testHandleLivez() {
        // given
        stubHttp1Request(serverRequest);
        Mockito.when(serverResponse.status(200)).thenReturn(serverResponse);
        Mockito.when(serverResponse.header(CONNECTION_CLOSE)).thenReturn(serverResponse);
        Mockito.doNothing().when(serverResponse).send("OK");

        BlockNodeContext context = Mockito.mock(BlockNodeContext.class);
        HealthFacility healthFacility = Mockito.mock(HealthFacility.class);
        Mockito.when(healthFacility.isRunning()).thenReturn(true);
        Mockito.when(context.serverHealth()).thenReturn(healthFacility);
        setupContextConfig(context);

        // when
        HealthServicePlugin healthServicePlugin = new HealthServicePlugin();
        healthServicePlugin.init(context, serviceBuilder);
        healthServicePlugin.handleLivez(serverRequest, serverResponse);

        // then
        Mockito.verify(serverResponse, Mockito.times(1)).status(200);
        Mockito.verify(serverResponse, Mockito.times(1)).header(CONNECTION_CLOSE);
        Mockito.verify(serverResponse, Mockito.times(1)).send("OK");
    }

    /// HTTP/2 forbids the `Connection` header (RFC 9113 8.2.2); Helidon throws an
    /// `Http2Exception` if a handler sets it on an HTTP/2 response, so it must never be added
    /// for HTTP/2 requests.
    @Test
    public void testHandleLivez_http2RequestDoesNotSetConnectionClose() {
        // given
        Mockito.when(serverRequest.prologue())
                .thenReturn(HttpPrologue.create("HTTP/2.0", "HTTP", "2.0", Method.GET, "/", false));
        Mockito.when(serverResponse.status(200)).thenReturn(serverResponse);
        Mockito.doNothing().when(serverResponse).send("OK");

        BlockNodeContext context = Mockito.mock(BlockNodeContext.class);
        HealthFacility healthFacility = Mockito.mock(HealthFacility.class);
        Mockito.when(healthFacility.isRunning()).thenReturn(true);
        Mockito.when(context.serverHealth()).thenReturn(healthFacility);
        setupContextConfig(context);

        // when
        HealthServicePlugin healthServicePlugin = new HealthServicePlugin();
        healthServicePlugin.init(context, serviceBuilder);
        healthServicePlugin.handleLivez(serverRequest, serverResponse);

        // then
        Mockito.verify(serverResponse, Mockito.never()).header(CONNECTION_CLOSE);
        Mockito.verify(serverResponse, Mockito.times(1)).send("OK");
    }

    @Test
    public void testHandleLivez_notRunning() {
        // given
        stubHttp1Request(serverRequest);
        Mockito.when(serverResponse.status(503)).thenReturn(serverResponse);
        Mockito.when(serverResponse.header(CONNECTION_CLOSE)).thenReturn(serverResponse);

        BlockNodeContext context = Mockito.mock(BlockNodeContext.class);
        HealthFacility healthFacility = Mockito.mock(HealthFacility.class);
        Mockito.when(healthFacility.isRunning()).thenReturn(false);
        Mockito.when(context.serverHealth()).thenReturn(healthFacility);
        setupContextConfig(context);

        // when
        HealthServicePlugin healthServicePlugin = new HealthServicePlugin();
        healthServicePlugin.init(context, serviceBuilder);
        healthServicePlugin.handleLivez(serverRequest, serverResponse);

        // then
        Mockito.verify(serverResponse, Mockito.times(1)).status(503);
        Mockito.verify(serverResponse, Mockito.times(1)).header(CONNECTION_CLOSE);
        Mockito.verify(serverResponse, Mockito.times(1)).send("Service is not running");
    }

    @Test
    public void testHandleReadyz() {
        // given
        stubHttp1Request(serverRequest);
        Mockito.when(serverResponse.status(200)).thenReturn(serverResponse);
        Mockito.when(serverResponse.header(CONNECTION_CLOSE)).thenReturn(serverResponse);
        Mockito.doNothing().when(serverResponse).send("OK");

        BlockNodeContext context = Mockito.mock(BlockNodeContext.class);
        HealthFacility healthFacility = Mockito.mock(HealthFacility.class);
        Mockito.when(healthFacility.isRunning()).thenReturn(true);
        Mockito.when(context.serverHealth()).thenReturn(healthFacility);
        setupContextConfig(context);

        // when
        HealthServicePlugin healthServicePlugin = new HealthServicePlugin();
        healthServicePlugin.init(context, serviceBuilder);
        healthServicePlugin.handleReadyz(serverRequest, serverResponse);

        // then
        Mockito.verify(serverResponse, Mockito.times(1)).status(200);
        Mockito.verify(serverResponse, Mockito.times(1)).header(CONNECTION_CLOSE);
        Mockito.verify(serverResponse, Mockito.times(1)).send("OK");
    }

    @Test
    public void testHandleReadyz_notRunning() {
        // given
        stubHttp1Request(serverRequest);
        Mockito.when(serverResponse.status(503)).thenReturn(serverResponse);
        Mockito.when(serverResponse.header(CONNECTION_CLOSE)).thenReturn(serverResponse);

        BlockNodeContext context = Mockito.mock(BlockNodeContext.class);
        HealthFacility healthFacility = Mockito.mock(HealthFacility.class);
        Mockito.when(healthFacility.isRunning()).thenReturn(false);
        Mockito.when(context.serverHealth()).thenReturn(healthFacility);
        setupContextConfig(context);

        // when
        HealthServicePlugin healthServicePlugin = new HealthServicePlugin();
        healthServicePlugin.init(context, serviceBuilder);
        healthServicePlugin.handleReadyz(serverRequest, serverResponse);

        // then
        Mockito.verify(serverResponse, Mockito.times(1)).status(503);
        Mockito.verify(serverResponse, Mockito.times(1)).header(CONNECTION_CLOSE);
        Mockito.verify(serverResponse, Mockito.times(1)).send("Service is not running");
    }

    /** Finds the {@link ServiceWithPath} registered at {@code path} across every port in the map. */
    private static ServiceWithPath findServiceByPath(
            final TreeMap<Integer, ServiceWithPath[]> services, final String path) {
        return services.values().stream()
                .flatMap(Arrays::stream)
                .filter(service -> service.path().equals(path))
                .findFirst()
                .orElse(null);
    }

    @Test
    public void testRouting() {
        // given
        @SuppressWarnings("unchecked")
        ArgumentCaptor<TreeMap<Integer, ServiceWithPath[]>> servicesCaptor = ArgumentCaptor.forClass(TreeMap.class);
        HttpRules httpRules = Mockito.mock(HttpRules.class);
        Mockito.when(httpRules.get(ArgumentMatchers.anyString(), ArgumentMatchers.any()))
                .thenReturn(httpRules);
        BlockNodeContext context = Mockito.mock(BlockNodeContext.class);
        setupContextConfig(context);

        // when
        HealthServicePlugin healthServicePlugin = new HealthServicePlugin();
        healthServicePlugin.init(context, serviceBuilder);

        // then: the health services are registered on a dedicated web server via registerHttpNewServer
        Mockito.verify(serviceBuilder, Mockito.times(1))
                .registerHttpNewServer(servicesCaptor.capture(), Mockito.any(), Mockito.any(), Mockito.any());
        ServiceWithPath healthService = findServiceByPath(servicesCaptor.getValue(), HEALTHZ_PATH);
        assertNotNull(healthService);

        // the health service's routing registers both the readiness and liveness paths
        healthService.services()[0].routing(httpRules);
        Mockito.verify(httpRules, Mockito.times(1)).get(ArgumentMatchers.eq(READYZ_PATH), ArgumentMatchers.any());
        Mockito.verify(httpRules, Mockito.times(1)).get(ArgumentMatchers.eq(LIVEZ_PATH), ArgumentMatchers.any());
    }

    @Test
    public void testStatuszRouting() {
        @SuppressWarnings("unchecked")
        ArgumentCaptor<TreeMap<Integer, ServiceWithPath[]>> servicesCaptor = ArgumentCaptor.forClass(TreeMap.class);
        HttpRules httpRules = Mockito.mock(HttpRules.class);
        Mockito.when(httpRules.get(ArgumentMatchers.anyString(), ArgumentMatchers.any()))
                .thenReturn(httpRules);
        BlockNodeContext context = Mockito.mock(BlockNodeContext.class);
        setupContextConfig(context);

        HealthServicePlugin healthServicePlugin = new HealthServicePlugin();
        healthServicePlugin.init(context, serviceBuilder);

        // the /statusz service is registered on the same dedicated web server with a wildcard route
        Mockito.verify(serviceBuilder, Mockito.times(1))
                .registerHttpNewServer(servicesCaptor.capture(), Mockito.any(), Mockito.any(), Mockito.any());
        ServiceWithPath statuszService = findServiceByPath(servicesCaptor.getValue(), STATUSZ_PATH);
        assertNotNull(statuszService);
        statuszService.services()[0].routing(httpRules);
        Mockito.verify(httpRules, Mockito.times(1)).get(ArgumentMatchers.eq("*"), ArgumentMatchers.any());
    }

    @Test
    public void testHandleStatuszInbound() throws ParseException {
        BlockNodeContext context = Mockito.mock(BlockNodeContext.class);
        Mockito.when(context.applicationStateFacility()).thenReturn(new TestApplicationStateFacility());
        setupContextConfig(context);

        HealthServicePlugin healthServicePlugin = new HealthServicePlugin();
        healthServicePlugin.init(context, serviceBuilder);

        TestServerResponse response = new TestServerResponse();
        healthServicePlugin.handleStatusz(new TestServerRequest("/statusz/inbound"), response);

        assertEquals(200, response.sentStatus());
        assertEquals("application/json", response.contentType());
        assertTrue(response.hasHeader(CONNECTION_CLOSE));
        NetworkData sent =
                NetworkData.JSON.parse(Bytes.wrap(((String) response.sentEntity()).getBytes(StandardCharsets.UTF_8)));
        assertFalse(sent.activeEndpoints().isEmpty());
    }

    @Test
    public void testHandleStatuszOutbound() throws ParseException {
        BlockNodeContext context = Mockito.mock(BlockNodeContext.class);
        Mockito.when(context.applicationStateFacility()).thenReturn(new TestApplicationStateFacility());
        setupContextConfig(context);

        HealthServicePlugin healthServicePlugin = new HealthServicePlugin();
        healthServicePlugin.init(context, serviceBuilder);

        TestServerResponse response = new TestServerResponse();
        healthServicePlugin.handleStatusz(new TestServerRequest("/statusz/outbound"), response);

        assertEquals(200, response.sentStatus());
        assertEquals("application/json", response.contentType());
        assertTrue(response.hasHeader(CONNECTION_CLOSE));
        NetworkData sent =
                NetworkData.JSON.parse(Bytes.wrap(((String) response.sentEntity()).getBytes(StandardCharsets.UTF_8)));
        assertFalse(sent.activeEndpoints().isEmpty());
    }

    @Test
    public void testHandleStatuszUnknownSubpath() {
        BlockNodeContext context = Mockito.mock(BlockNodeContext.class);
        Mockito.when(context.applicationStateFacility()).thenReturn(new TestApplicationStateFacility());
        setupContextConfig(context);

        HealthServicePlugin healthServicePlugin = new HealthServicePlugin();
        healthServicePlugin.init(context, serviceBuilder);

        TestServerResponse response = new TestServerResponse();
        healthServicePlugin.handleStatusz(new TestServerRequest("/statusz/bogus"), response);

        assertEquals(404, response.sentStatus());
        assertTrue(response.hasHeader(CONNECTION_CLOSE));
    }

    @Test
    public void testHandleLivez_recordsRequestMetric() {
        // given
        stubHttp1Request(serverRequest);
        Mockito.when(serverResponse.status(200)).thenReturn(serverResponse);
        Mockito.when(serverResponse.header(CONNECTION_CLOSE)).thenReturn(serverResponse);

        BlockNodeContext context = Mockito.mock(BlockNodeContext.class);
        HealthFacility healthFacility = Mockito.mock(HealthFacility.class);
        Mockito.when(healthFacility.isRunning()).thenReturn(true);
        Mockito.when(context.serverHealth()).thenReturn(healthFacility);
        setupContextConfig(context);

        HealthServicePlugin healthServicePlugin = new HealthServicePlugin();
        healthServicePlugin.init(context, serviceBuilder);

        // when
        healthServicePlugin.handleLivez(serverRequest, serverResponse);

        // then
        assertEquals(1, metricsExporter.getMetricValue(METRIC_HEALTH_REQUESTS.name()));
    }

    @Test
    public void testHandleReadyz_notRunning_recordsRequestMetric() {
        // given
        stubHttp1Request(serverRequest);
        Mockito.when(serverResponse.status(503)).thenReturn(serverResponse);
        Mockito.when(serverResponse.header(CONNECTION_CLOSE)).thenReturn(serverResponse);

        BlockNodeContext context = Mockito.mock(BlockNodeContext.class);
        HealthFacility healthFacility = Mockito.mock(HealthFacility.class);
        Mockito.when(healthFacility.isRunning()).thenReturn(false);
        Mockito.when(context.serverHealth()).thenReturn(healthFacility);
        setupContextConfig(context);

        HealthServicePlugin healthServicePlugin = new HealthServicePlugin();
        healthServicePlugin.init(context, serviceBuilder);

        // when
        healthServicePlugin.handleReadyz(serverRequest, serverResponse);

        // then
        assertEquals(1, metricsExporter.getMetricValue(METRIC_HEALTH_REQUESTS.name()));
    }

    @Test
    public void testHandleStatusz_doesNotRecordRequestMetric() {
        // given: a running node and one health check so the request counter has a value to compare
        stubHttp1Request(serverRequest);
        Mockito.when(serverResponse.status(200)).thenReturn(serverResponse);
        Mockito.when(serverResponse.header(CONNECTION_CLOSE)).thenReturn(serverResponse);

        BlockNodeContext context = Mockito.mock(BlockNodeContext.class);
        HealthFacility healthFacility = Mockito.mock(HealthFacility.class);
        Mockito.when(healthFacility.isRunning()).thenReturn(true);
        Mockito.when(context.serverHealth()).thenReturn(healthFacility);
        Mockito.when(context.applicationStateFacility()).thenReturn(new TestApplicationStateFacility());
        setupContextConfig(context);

        HealthServicePlugin healthServicePlugin = new HealthServicePlugin();
        healthServicePlugin.init(context, serviceBuilder);
        healthServicePlugin.handleLivez(serverRequest, serverResponse);

        // when
        healthServicePlugin.handleStatusz(new TestServerRequest("/statusz/bogus"), new TestServerResponse());

        // then: statusz serves statistics, not health checks, so it leaves the counter unchanged
        assertEquals(1, metricsExporter.getMetricValue(METRIC_HEALTH_REQUESTS.name()));
    }
}
