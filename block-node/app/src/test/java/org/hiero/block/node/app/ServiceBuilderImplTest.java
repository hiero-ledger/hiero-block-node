// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hedera.pbj.grpc.helidon.PbjRouting;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import io.helidon.common.socket.SocketOptions;
import io.helidon.webserver.http.HttpRouting;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http2.Http2Config;
import java.util.List;
import java.util.Map;
import org.hiero.block.node.app.config.GlobalThrottleConfig;
import org.hiero.block.node.app.config.ServerConfig;
import org.hiero.block.node.app.fixtures.TestMetricsExporter;
import org.hiero.block.node.spi.threading.ThreadPoolManager;
import org.hiero.block.node.spi.throttle.PerClientThrottleSettings;
import org.hiero.block.node.spi.throttle.ThrottleExempt;
import org.hiero.block.node.spi.throttle.ThrottleSpec;
import org.hiero.block.node.spi.throttle.ThrottledServiceInterface;
import org.hiero.metrics.core.MetricRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Tests for {@link ServiceBuilderImpl} class which implements the {@link org.hiero.block.node.spi.ServiceBuilder}
 * interface for registering HTTP and PBJ GRPC services.
 */
class ServiceBuilderImplTest {

    private static final int PUBLISHER_PORT = 40840;
    private static final int CONSUMER_PORT = 40940;
    private static final int CUSTOM_PORT = 12345;

    private ServiceBuilderImpl serviceBuilder;

    @BeforeEach
    void setUp() {
        final Http2Config http2Config = Http2Config.builder().build();
        final SocketOptions socketOptions = SocketOptions.builder().build();
        ServerConfig testConfig = new ServerConfig(0, 0, 0, PUBLISHER_PORT, 0, 0, 0, 0, false, 0, 0);
        final GlobalThrottleConfig globalThrottleConfig = new GlobalThrottleConfig(1000, 30, 5);
        final MetricRegistry metricRegistry = MetricRegistry.builder()
                .setMetricsExporter(new TestMetricsExporter())
                .build();
        final ThreadPoolManager threadPoolManager = mock(ThreadPoolManager.class);
        // Only exercised by throttled-registration tests, which trigger the lazily-started
        // stale-client sweep; a mock executor is enough since these tests don't assert on sweeps.
        when(threadPoolManager.createVirtualThreadScheduledExecutor(
                        org.mockito.ArgumentMatchers.anyInt(),
                        org.mockito.ArgumentMatchers.any(),
                        org.mockito.ArgumentMatchers.any()))
                .thenReturn(mock(java.util.concurrent.ScheduledExecutorService.class));
        serviceBuilder = new ServiceBuilderImpl(
                testConfig, http2Config, socketOptions, globalThrottleConfig, metricRegistry, threadPoolManager);
    }

    @Test
    @DisplayName("registerGrpcService wraps a ThrottleSpec service using its own reported settings")
    void registerGrpcService_throttleSpec_wrapsService() {
        final ServiceInterface testService =
                new TestThrottledService("TestService", new PerClientThrottleSettings(10, 5, 3), 100);
        final PbjRouting.Builder spyBuilder = injectGrpcBuilderSpy(CONSUMER_PORT);

        serviceBuilder.registerGrpcService(CONSUMER_PORT, testService);

        final ArgumentCaptor<ServiceInterface> registered = ArgumentCaptor.forClass(ServiceInterface.class);
        verify(spyBuilder).service(registered.capture());
        assertInstanceOf(ThrottledServiceInterface.class, registered.getValue());
        assertNotSame(testService, registered.getValue(), "the raw service must be wrapped, not registered as-is");
    }

    @Test
    @DisplayName("registerGrpcService registers a ThrottleExempt service as-is, without wrapping")
    void registerGrpcService_throttleExempt_registersRawServiceWithoutWrapping() {
        final ServiceInterface testService = new TestExemptService("TestExemptService");
        final PbjRouting.Builder spyBuilder = injectGrpcBuilderSpy(CONSUMER_PORT);

        serviceBuilder.registerGrpcService(CONSUMER_PORT, testService);

        verify(spyBuilder).service(eq(testService));
    }

    /// A minimal, hand-written [ServiceInterface] + [ThrottleSpec] test double. Deliberately not a
    /// Mockito mock with `extraInterfaces`: the generated mock class would live in
    /// `ServiceInterface`'s own module (`com.hedera.pbj.runtime`), which does not read
    /// `org.hiero.block.node.spi` — so Mockito cannot make it implement `ThrottleSpec` across that
    /// module boundary. A real class compiled into this module has no such restriction.
    private static final class TestThrottledService implements ServiceInterface, ThrottleSpec {
        private final String serviceName;
        private final PerClientThrottleSettings perClientSettings;
        private final int globalConcurrencyCeiling;

        TestThrottledService(
                final String serviceName,
                final PerClientThrottleSettings perClientSettings,
                final int globalConcurrencyCeiling) {
            this.serviceName = serviceName;
            this.perClientSettings = perClientSettings;
            this.globalConcurrencyCeiling = globalConcurrencyCeiling;
        }

        @Override
        public String serviceName() {
            return serviceName;
        }

        @Override
        public String fullName() {
            return serviceName;
        }

        @Override
        public List<Method> methods() {
            return List.of();
        }

        @Override
        public com.hedera.pbj.runtime.grpc.Pipeline<? super com.hedera.pbj.runtime.io.buffer.Bytes> open(
                final Method method,
                final RequestOptions opts,
                final com.hedera.pbj.runtime.grpc.Pipeline<? super com.hedera.pbj.runtime.io.buffer.Bytes> responses) {
            throw new UnsupportedOperationException("not exercised by these registration-only tests");
        }

        @Override
        public PerClientThrottleSettings perClientSettings() {
            return perClientSettings;
        }

        @Override
        public int globalConcurrencyCeiling() {
            return globalConcurrencyCeiling;
        }
    }

    /// A minimal, hand-written [ServiceInterface] + [ThrottleExempt] test double; see
    /// [TestThrottledService] for why this isn't a Mockito `extraInterfaces` mock.
    private static final class TestExemptService implements ServiceInterface, ThrottleExempt {
        private final String serviceName;

        TestExemptService(final String serviceName) {
            this.serviceName = serviceName;
        }

        @Override
        public String serviceName() {
            return serviceName;
        }

        @Override
        public String fullName() {
            return serviceName;
        }

        @Override
        public List<Method> methods() {
            return List.of();
        }

        @Override
        public com.hedera.pbj.runtime.grpc.Pipeline<? super com.hedera.pbj.runtime.io.buffer.Bytes> open(
                final Method method,
                final RequestOptions opts,
                final com.hedera.pbj.runtime.grpc.Pipeline<? super com.hedera.pbj.runtime.io.buffer.Bytes> responses) {
            throw new UnsupportedOperationException("not exercised by these registration-only tests");
        }
    }

    @Test
    @DisplayName("httpRoutingBuilders should return a non-null map")
    void httpRoutingBuilders_shouldReturnBuilder() {
        assertNotNull(serviceBuilder.httpRoutingBuilders(), "HTTP routing builders map should not be null");
    }

    @Test
    @DisplayName("grpcRoutingBuilders should return a non-null map")
    void grpcRoutingBuilders_shouldReturnBuilder() {
        assertNotNull(serviceBuilder.grpcRoutingBuilders(), "GRPC routing builders map should not be null");
    }

    @Test
    @DisplayName("registerHttpService should register a single HTTP service at the given path")
    void registerHttpService_withSingleService() {
        final String path = "/api/test";
        final HttpService mockService = mock(HttpService.class);
        final HttpRouting.Builder spyBuilder = injectHttpBuilderSpy(CONSUMER_PORT);

        serviceBuilder.registerHttpService(path, CONSUMER_PORT, mockService);

        verify(spyBuilder).register(eq(path), eq(mockService));
    }

    @Test
    @DisplayName("registerHttpService should register multiple HTTP services at the given path")
    void registerHttpService_withMultipleServices() {
        final String path = "/api/test";
        final HttpService mockService1 = mock(HttpService.class);
        final HttpService mockService2 = mock(HttpService.class);
        final HttpService mockService3 = mock(HttpService.class);
        final HttpRouting.Builder spyBuilder = injectHttpBuilderSpy(CONSUMER_PORT);

        serviceBuilder.registerHttpService(path, CONSUMER_PORT, mockService1, mockService2, mockService3);

        verify(spyBuilder).register(eq(path), eq(mockService1), eq(mockService2), eq(mockService3));
    }

    @Test
    @DisplayName("registerHttpService should handle empty service array")
    void registerHttpService_withEmptyServices() {
        final String path = "/api/test";
        final HttpService[] emptyServices = new HttpService[0];
        final HttpRouting.Builder spyBuilder = injectHttpBuilderSpy(CONSUMER_PORT);

        serviceBuilder.registerHttpService(path, CONSUMER_PORT, emptyServices);

        verify(spyBuilder).register(eq(path), eq(emptyServices));
    }

    @Test
    @DisplayName("registerGrpcService should register a GRPC service")
    void registerGrpcService_shouldRegisterService() {
        final ServiceInterface mockService = mock(ServiceInterface.class);
        final PbjRouting.Builder spyBuilder = injectGrpcBuilderSpy(CONSUMER_PORT);

        serviceBuilder.registerGrpcService(CONSUMER_PORT, mockService);

        verify(spyBuilder).service(eq(mockService));
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    @DisplayName("registerGrpcService should throw NullPointerException when passed null")
    void registerGrpcService_shouldThrowNPEForNullService() {
        assertThrows(
                NullPointerException.class,
                () -> serviceBuilder.registerGrpcService(CONSUMER_PORT, null),
                "registerGrpcService should throw NullPointerException when passed null");
    }

    @Test
    @DisplayName("Multiple HTTP service registrations on the same port should be correctly handled")
    void multipleHttpRegistrations_shouldBeHandledCorrectly() {
        final String path1 = "/api/test1";
        final String path2 = "/api/test2";
        final HttpService mockService1 = mock(HttpService.class);
        final HttpService mockService2 = mock(HttpService.class);
        final HttpRouting.Builder spyBuilder = injectHttpBuilderSpy(CONSUMER_PORT);

        serviceBuilder.registerHttpService(path1, CONSUMER_PORT, mockService1);
        serviceBuilder.registerHttpService(path2, CONSUMER_PORT, mockService2);

        verify(spyBuilder).register(eq(path1), eq(mockService1));
        verify(spyBuilder).register(eq(path2), eq(mockService2));
    }

    @Test
    @DisplayName("Multiple GRPC service registrations on the same port should be correctly handled")
    void multipleGrpcRegistrations_shouldBeHandledCorrectly() {
        final ServiceInterface mockService1 = mock(ServiceInterface.class);
        final ServiceInterface mockService2 = mock(ServiceInterface.class);
        final PbjRouting.Builder spyBuilder = injectGrpcBuilderSpy(CONSUMER_PORT);

        serviceBuilder.registerGrpcService(CONSUMER_PORT, mockService1);
        serviceBuilder.registerGrpcService(CONSUMER_PORT, mockService2);

        verify(spyBuilder).service(eq(mockService1));
        verify(spyBuilder).service(eq(mockService2));
    }

    @Test
    @DisplayName("httpRoutingBuilders should return non-null map")
    void httpRoutingBuilders_returnsNonNullMap() {
        assertNotNull(serviceBuilder.httpRoutingBuilders());
    }

    @Test
    @DisplayName("grpcRoutingBuilders should return non-null map")
    void grpcRoutingBuilders_returnsNonNullMap() {
        assertNotNull(serviceBuilder.grpcRoutingBuilders());
    }

    @Test
    @DisplayName("registerHttpService creates an entry keyed by the given port")
    void registerHttpService_createsEntry() {
        final HttpService mockService = mock(HttpService.class);

        serviceBuilder.registerHttpService("/api/test", CONSUMER_PORT, mockService);

        assertNotNull(serviceBuilder.httpRoutingBuilders().get(CONSUMER_PORT));
    }

    @Test
    @DisplayName("registerGrpcService creates an entry keyed by the given port")
    void registerGrpcService_createsEntry() {
        final ServiceInterface mockService = mock(ServiceInterface.class);

        serviceBuilder.registerGrpcService(PUBLISHER_PORT, mockService);

        assertNotNull(serviceBuilder.grpcRoutingBuilders().get(PUBLISHER_PORT));
    }

    @Test
    @DisplayName("registerGrpcService on publisher port does not populate consumer port")
    void registerGrpcService_publisherPort_doesNotPopulateConsumerPort() {
        final ServiceInterface mockService = mock(ServiceInterface.class);

        serviceBuilder.registerGrpcService(PUBLISHER_PORT, mockService);

        assertTrue(serviceBuilder.grpcRoutingBuilders().containsKey(PUBLISHER_PORT));
        assertFalse(serviceBuilder.grpcRoutingBuilders().containsKey(CONSUMER_PORT));
    }

    @Test
    @DisplayName("registerGrpcService throws NullPointerException for null service")
    void registerGrpcService_nullService_throwsNPE() {
        assertThrows(NullPointerException.class, () -> serviceBuilder.registerGrpcService(CONSUMER_PORT, null));
    }

    @Test
    @DisplayName("Multiple gRPC registrations on the same port reuse the same builder instance")
    void multipleGrpcRegistrations_samePort_reuseBuilder() {
        final ServiceInterface mockService1 = mock(ServiceInterface.class);
        final ServiceInterface mockService2 = mock(ServiceInterface.class);

        serviceBuilder.registerGrpcService(CONSUMER_PORT, mockService1);
        final PbjRouting.Builder builderAfterFirst =
                serviceBuilder.grpcRoutingBuilders().get(CONSUMER_PORT);

        serviceBuilder.registerGrpcService(CONSUMER_PORT, mockService2);
        final PbjRouting.Builder builderAfterSecond =
                serviceBuilder.grpcRoutingBuilders().get(CONSUMER_PORT);

        assertSame(builderAfterFirst, builderAfterSecond, "Same port should reuse the same routing builder");
    }

    @Test
    @DisplayName("Multiple HTTP registrations on the same port reuse the same builder instance")
    void multipleHttpRegistrations_reuseBuilder() {
        final HttpService mockService1 = mock(HttpService.class);
        final HttpService mockService2 = mock(HttpService.class);

        serviceBuilder.registerHttpService("/a", CONSUMER_PORT, mockService1);
        final HttpRouting.Builder builderAfterFirst =
                serviceBuilder.httpRoutingBuilders().get(CONSUMER_PORT);

        serviceBuilder.registerHttpService("/b", CONSUMER_PORT, mockService2);
        final HttpRouting.Builder builderAfterSecond =
                serviceBuilder.httpRoutingBuilders().get(CONSUMER_PORT);

        assertSame(builderAfterFirst, builderAfterSecond, "Same port should reuse the same routing builder");
    }

    @Test
    @DisplayName("gRPC registrations on different ports yield independent builders")
    void grpcRegistrations_differentPorts_haveIndependentBuilders() {
        final ServiceInterface mockService = mock(ServiceInterface.class);

        serviceBuilder.registerGrpcService(PUBLISHER_PORT, mockService);
        serviceBuilder.registerGrpcService(CONSUMER_PORT, mockService);

        assertNotSame(
                serviceBuilder.grpcRoutingBuilders().get(PUBLISHER_PORT),
                serviceBuilder.grpcRoutingBuilders().get(CONSUMER_PORT),
                "Different ports must use independent routing builders");
    }

    @Test
    @DisplayName("null port for registerGrpcService resolves to default port")
    void registerGrpcService_nullPort_resolvesToDefault() {
        final ServiceInterface mockService = mock(ServiceInterface.class);

        serviceBuilder.registerGrpcService(null, mockService);

        assertNotNull(
                serviceBuilder.grpcRoutingBuilders().get(PUBLISHER_PORT), "null port must resolve to default port");
        assertFalse(serviceBuilder.grpcRoutingBuilders().containsKey(CONSUMER_PORT));
    }

    @Test
    @DisplayName("null port for registerHttpService resolves to default port")
    void registerHttpService_nullPort_resolvesToDefault() {
        final HttpService mockService = mock(HttpService.class);

        serviceBuilder.registerHttpService("/api/test", null, mockService);

        assertNotNull(
                serviceBuilder.httpRoutingBuilders().get(PUBLISHER_PORT), "null port must resolve to default port");
        assertFalse(serviceBuilder.httpRoutingBuilders().containsKey(CONSUMER_PORT));
    }

    @Test
    @DisplayName("gRPC service registered on a custom port gets its own builder")
    void registerGrpcService_customPort_getsOwnBuilder() {
        final ServiceInterface mockService = mock(ServiceInterface.class);

        serviceBuilder.registerGrpcService(PUBLISHER_PORT, mockService);
        serviceBuilder.registerGrpcService(CUSTOM_PORT, mockService);

        assertNotNull(serviceBuilder.grpcRoutingBuilders().get(CUSTOM_PORT));
        assertNotSame(
                serviceBuilder.grpcRoutingBuilders().get(PUBLISHER_PORT),
                serviceBuilder.grpcRoutingBuilders().get(CUSTOM_PORT),
                "Custom port must have its own independent routing builder");
    }

    private HttpRouting.Builder injectHttpBuilderSpy(final int port) {
        try {
            final java.lang.reflect.Field field = ServiceBuilderImpl.class.getDeclaredField("httpBuilders");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            final Map<Integer, HttpRouting.Builder> map = (Map<Integer, HttpRouting.Builder>) field.get(serviceBuilder);
            final HttpRouting.Builder spyBuilder = spy(HttpRouting.builder());
            map.put(port, spyBuilder);
            return spyBuilder;
        } catch (final Exception e) {
            fail("Failed to inject HTTP builder spy: " + e.getMessage());
            throw new AssertionError("unreachable");
        }
    }

    private PbjRouting.Builder injectGrpcBuilderSpy(final int port) {
        try {
            final java.lang.reflect.Field field = ServiceBuilderImpl.class.getDeclaredField("grpcBuilders");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            final Map<Integer, PbjRouting.Builder> map = (Map<Integer, PbjRouting.Builder>) field.get(serviceBuilder);
            final PbjRouting.Builder spyBuilder = spy(PbjRouting.builder());
            map.put(port, spyBuilder);
            return spyBuilder;
        } catch (final Exception e) {
            fail("Failed to inject gRPC builder spy: " + e.getMessage());
            throw new AssertionError("unreachable");
        }
    }
}
