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
import org.hiero.block.node.spi.ServiceBuilder.Socket;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ServiceBuilderImpl} class which implements the {@link org.hiero.block.node.spi.ServiceBuilder}
 * interface for registering HTTP and PBJ GRPC services.
 */
class ServiceBuilderImplTest {

    private ServiceBuilderImpl serviceBuilder;

    @BeforeEach
    void setUp() {
        serviceBuilder = new ServiceBuilderImpl();
    }

    /**
     * Tests that the httpRoutingBuilders map is properly initialized and returned.
     */
    @Test
    @DisplayName("httpRoutingBuilder should return a non-null HttpRouting.Builder")
    void httpRoutingBuilder_shouldReturnBuilder() {
        assertNotNull(serviceBuilder.httpRoutingBuilders(), "HTTP routing builders map should not be null");
    }

    /**
     * Tests that the grpcRoutingBuilders map is properly initialized and returned.
     */
    @Test
    @DisplayName("grpcRoutingBuilder should return a non-null PbjRouting.Builder")
    void grpcRoutingBuilder_shouldReturnBuilder() {
        assertNotNull(serviceBuilder.grpcRoutingBuilders(), "GRPC routing builders map should not be null");
    }

    /**
     * Tests the registerHttpService method with a single service.
     */
    @Test
    @DisplayName("registerHttpService should register a single HTTP service at the given path")
    void registerHttpService_withSingleService() {
        final String path = "/api/test";
        final HttpService mockService = mock(HttpService.class);
        final HttpRouting.Builder spyBuilder = injectHttpBuilderSpy(Socket.CONSUMER);

        serviceBuilder.registerHttpService(path, mockService);

        verify(spyBuilder).register(eq(path), eq(mockService));
    }

    /**
     * Tests the registerHttpService method with multiple services.
     */
    @Test
    @DisplayName("registerHttpService should register multiple HTTP services at the given path")
    void registerHttpService_withMultipleServices() {
        final String path = "/api/test";
        final HttpService mockService1 = mock(HttpService.class);
        final HttpService mockService2 = mock(HttpService.class);
        final HttpService mockService3 = mock(HttpService.class);
        final HttpRouting.Builder spyBuilder = injectHttpBuilderSpy(Socket.CONSUMER);

        serviceBuilder.registerHttpService(path, mockService1, mockService2, mockService3);

        verify(spyBuilder).register(eq(path), eq(mockService1), eq(mockService2), eq(mockService3));
    }

    /**
     * Tests the registerHttpService method with an empty service array.
     */
    @Test
    @DisplayName("registerHttpService should handle empty service array")
    void registerHttpService_withEmptyServices() {
        final String path = "/api/test";
        final HttpService[] emptyServices = new HttpService[0];
        final HttpRouting.Builder spyBuilder = injectHttpBuilderSpy(Socket.CONSUMER);

        serviceBuilder.registerHttpService(path, emptyServices);

        verify(spyBuilder).register(eq(path), eq(emptyServices));
    }

    /**
     * Tests the registerGrpcService method.
     */
    @Test
    @DisplayName("registerGrpcService should register a GRPC service")
    void registerGrpcService_shouldRegisterService() {
        final ServiceInterface mockService = mock(ServiceInterface.class);
        final PbjRouting.Builder spyBuilder = injectGrpcBuilderSpy(Socket.CONSUMER);

        serviceBuilder.registerGrpcService(mockService, Socket.CONSUMER);

        verify(spyBuilder).service(eq(mockService));
    }

    /**
     * Tests that registerGrpcService throws NullPointerException when passed a null service.
     */
    @SuppressWarnings("DataFlowIssue")
    @Test
    @DisplayName("registerGrpcService should throw NullPointerException when passed null")
    void registerGrpcService_shouldThrowNPEForNullService() {
        assertThrows(
                NullPointerException.class,
                () -> serviceBuilder.registerGrpcService(null, Socket.CONSUMER),
                "registerGrpcService should throw NullPointerException when passed null");
    }

    /**
     * Tests that multiple registrations of HTTP services work correctly.
     */
    @Test
    @DisplayName("Multiple HTTP service registrations should be correctly handled")
    void multipleHttpRegistrations_shouldBeHandledCorrectly() {
        final String path1 = "/api/test1";
        final String path2 = "/api/test2";
        final HttpService mockService1 = mock(HttpService.class);
        final HttpService mockService2 = mock(HttpService.class);
        final HttpRouting.Builder spyBuilder = injectHttpBuilderSpy(Socket.CONSUMER);

        serviceBuilder.registerHttpService(path1, mockService1);
        serviceBuilder.registerHttpService(path2, mockService2);

        verify(spyBuilder).register(eq(path1), eq(mockService1));
        verify(spyBuilder).register(eq(path2), eq(mockService2));
    }

    /**
     * Tests that multiple registrations of GRPC services work correctly.
     */
    @Test
    @DisplayName("Multiple GRPC service registrations should be correctly handled")
    void multipleGrpcRegistrations_shouldBeHandledCorrectly() {
        final ServiceInterface mockService1 = mock(ServiceInterface.class);
        final ServiceInterface mockService2 = mock(ServiceInterface.class);
        final PbjRouting.Builder spyBuilder = injectGrpcBuilderSpy(Socket.CONSUMER);

        serviceBuilder.registerGrpcService(mockService1, Socket.CONSUMER);
        serviceBuilder.registerGrpcService(mockService2, Socket.CONSUMER);

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
    @DisplayName("registerHttpService creates an entry for Socket.CONSUMER")
    void registerHttpService_createsEntry() {
        final HttpService mockService = mock(HttpService.class);

        serviceBuilder.registerHttpService("/api/test", mockService);

        assertNotNull(serviceBuilder.httpRoutingBuilders().get(Socket.CONSUMER));
    }

    @Test
    @DisplayName("registerGrpcService with socket creates an entry for that socket")
    void registerGrpcService_socketAware_createsEntry() {
        final ServiceInterface mockService = mock(ServiceInterface.class);

        serviceBuilder.registerGrpcService(mockService, Socket.PUBLISHER);

        assertNotNull(serviceBuilder.grpcRoutingBuilders().get(Socket.PUBLISHER));
    }

    @Test
    @DisplayName("registerGrpcService on PUBLISHER socket does not populate CONSUMER socket")
    void registerGrpcService_publisherSocket_doesNotPopulateConsumerSocket() {
        final ServiceInterface mockService = mock(ServiceInterface.class);

        serviceBuilder.registerGrpcService(mockService, Socket.PUBLISHER);

        assertNotNull(serviceBuilder.grpcRoutingBuilders());
        assertTrue(serviceBuilder.grpcRoutingBuilders().containsKey(Socket.PUBLISHER));
        assertFalse(serviceBuilder.grpcRoutingBuilders().containsKey(Socket.CONSUMER));
    }

    @Test
    @DisplayName("registerGrpcService throws NullPointerException for null service")
    void registerGrpcService_nullService_throwsNPE() {
        assertThrows(NullPointerException.class, () -> serviceBuilder.registerGrpcService(null, Socket.CONSUMER));
    }

    @Test
    @DisplayName("Multiple gRPC registrations on the same socket reuse the same builder instance")
    void multipleGrpcRegistrations_sameSocket_reuseBuilder() {
        final ServiceInterface mockService1 = mock(ServiceInterface.class);
        final ServiceInterface mockService2 = mock(ServiceInterface.class);

        serviceBuilder.registerGrpcService(mockService1, Socket.CONSUMER);
        final PbjRouting.Builder builderAfterFirst =
                serviceBuilder.grpcRoutingBuilders().get(Socket.CONSUMER);

        serviceBuilder.registerGrpcService(mockService2, Socket.CONSUMER);
        final PbjRouting.Builder builderAfterSecond =
                serviceBuilder.grpcRoutingBuilders().get(Socket.CONSUMER);

        assertSame(builderAfterFirst, builderAfterSecond, "Same socket should reuse the same routing builder");
    }

    @Test
    @DisplayName("Multiple HTTP registrations reuse the same Socket.CONSUMER builder instance")
    void multipleHttpRegistrations_reuseBuilder() {
        final HttpService mockService1 = mock(HttpService.class);
        final HttpService mockService2 = mock(HttpService.class);

        serviceBuilder.registerHttpService("/a", mockService1);
        final HttpRouting.Builder builderAfterFirst =
                serviceBuilder.httpRoutingBuilders().get(Socket.CONSUMER);

        serviceBuilder.registerHttpService("/b", mockService2);
        final HttpRouting.Builder builderAfterSecond =
                serviceBuilder.httpRoutingBuilders().get(Socket.CONSUMER);

        assertSame(
                builderAfterFirst,
                builderAfterSecond,
                "Multiple HTTP registrations should reuse the same routing builder");
    }

    @Test
    @DisplayName("gRPC registrations on different sockets yield independent builders")
    void grpcRegistrations_differentSockets_haveIndependentBuilders() {
        final ServiceInterface mockService = mock(ServiceInterface.class);

        serviceBuilder.registerGrpcService(mockService, Socket.PUBLISHER);
        serviceBuilder.registerGrpcService(mockService, Socket.CONSUMER);

        assertNotSame(
                serviceBuilder.grpcRoutingBuilders().get(Socket.PUBLISHER),
                serviceBuilder.grpcRoutingBuilders().get(Socket.CONSUMER),
                "Different sockets must use independent routing builders");
    }

    private HttpRouting.Builder injectHttpBuilderSpy(final Socket socket) {
        try {
            final java.lang.reflect.Field field = ServiceBuilderImpl.class.getDeclaredField("httpBuilders");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            final Map<Socket, HttpRouting.Builder> map = (Map<Socket, HttpRouting.Builder>) field.get(serviceBuilder);
            final HttpRouting.Builder spyBuilder = spy(HttpRouting.builder());
            map.put(socket, spyBuilder);
            return spyBuilder;
        } catch (final Exception e) {
            fail("Failed to inject HTTP builder spy: " + e.getMessage());
            throw new AssertionError("unreachable");
        }
    }

    private PbjRouting.Builder injectGrpcBuilderSpy(final Socket socket) {
        try {
            final java.lang.reflect.Field field = ServiceBuilderImpl.class.getDeclaredField("grpcBuilders");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            final Map<Socket, PbjRouting.Builder> map = (Map<Socket, PbjRouting.Builder>) field.get(serviceBuilder);
            final PbjRouting.Builder spyBuilder = spy(PbjRouting.builder());
            map.put(socket, spyBuilder);
            return spyBuilder;
        } catch (final Exception e) {
            fail("Failed to inject gRPC builder spy: " + e.getMessage());
            throw new AssertionError("unreachable");
        }
    }
}
