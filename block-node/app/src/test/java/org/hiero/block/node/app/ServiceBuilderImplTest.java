// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import static org.junit.jupiter.api.Assertions.assertFalse;
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

import com.hedera.pbj.grpc.helidon.PbjRouting;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import io.helidon.webserver.http.HttpRouting;
import io.helidon.webserver.http.HttpService;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

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
        serviceBuilder = new ServiceBuilderImpl(PUBLISHER_PORT);
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

        serviceBuilder.registerGrpcService(mockService, CONSUMER_PORT);

        verify(spyBuilder).service(eq(mockService));
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    @DisplayName("registerGrpcService should throw NullPointerException when passed null")
    void registerGrpcService_shouldThrowNPEForNullService() {
        assertThrows(
                NullPointerException.class,
                () -> serviceBuilder.registerGrpcService(null, CONSUMER_PORT),
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

        serviceBuilder.registerGrpcService(mockService1, CONSUMER_PORT);
        serviceBuilder.registerGrpcService(mockService2, CONSUMER_PORT);

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

        serviceBuilder.registerGrpcService(mockService, PUBLISHER_PORT);

        assertNotNull(serviceBuilder.grpcRoutingBuilders().get(PUBLISHER_PORT));
    }

    @Test
    @DisplayName("registerGrpcService on publisher port does not populate consumer port")
    void registerGrpcService_publisherPort_doesNotPopulateConsumerPort() {
        final ServiceInterface mockService = mock(ServiceInterface.class);

        serviceBuilder.registerGrpcService(mockService, PUBLISHER_PORT);

        assertTrue(serviceBuilder.grpcRoutingBuilders().containsKey(PUBLISHER_PORT));
        assertFalse(serviceBuilder.grpcRoutingBuilders().containsKey(CONSUMER_PORT));
    }

    @Test
    @DisplayName("registerGrpcService throws NullPointerException for null service")
    void registerGrpcService_nullService_throwsNPE() {
        assertThrows(NullPointerException.class, () -> serviceBuilder.registerGrpcService(null, CONSUMER_PORT));
    }

    @Test
    @DisplayName("Multiple gRPC registrations on the same port reuse the same builder instance")
    void multipleGrpcRegistrations_samePort_reuseBuilder() {
        final ServiceInterface mockService1 = mock(ServiceInterface.class);
        final ServiceInterface mockService2 = mock(ServiceInterface.class);

        serviceBuilder.registerGrpcService(mockService1, CONSUMER_PORT);
        final PbjRouting.Builder builderAfterFirst =
                serviceBuilder.grpcRoutingBuilders().get(CONSUMER_PORT);

        serviceBuilder.registerGrpcService(mockService2, CONSUMER_PORT);
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

        serviceBuilder.registerGrpcService(mockService, PUBLISHER_PORT);
        serviceBuilder.registerGrpcService(mockService, CONSUMER_PORT);

        assertNotSame(
                serviceBuilder.grpcRoutingBuilders().get(PUBLISHER_PORT),
                serviceBuilder.grpcRoutingBuilders().get(CONSUMER_PORT),
                "Different ports must use independent routing builders");
    }

    @Test
    @DisplayName("null port for registerGrpcService resolves to default port")
    void registerGrpcService_nullPort_resolvesToDefault() {
        final ServiceInterface mockService = mock(ServiceInterface.class);

        serviceBuilder.registerGrpcService(mockService, null);

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

        serviceBuilder.registerGrpcService(mockService, PUBLISHER_PORT);
        serviceBuilder.registerGrpcService(mockService, CUSTOM_PORT);

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
