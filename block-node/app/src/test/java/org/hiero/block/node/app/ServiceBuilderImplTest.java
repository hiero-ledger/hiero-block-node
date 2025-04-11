// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.hedera.pbj.grpc.helidon.PbjRouting;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import io.helidon.webserver.http.HttpRouting;
import io.helidon.webserver.http.HttpService;
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
     * Tests that the httpRoutingBuilder is properly initialized and returned.
     */
    @Test
    @DisplayName("httpRoutingBuilder should return a non-null HttpRouting.Builder")
    void httpRoutingBuilder_shouldReturnBuilder() {
        final HttpRouting.Builder builder = serviceBuilder.httpRoutingBuilder();

        assertNotNull(builder, "HTTP routing builder should not be null");
    }

    /**
     * Tests that the grpcRoutingBuilder is properly initialized and returned.
     */
    @Test
    @DisplayName("grpcRoutingBuilder should return a non-null PbjRouting.Builder")
    void grpcRoutingBuilder_shouldReturnBuilder() {
        final PbjRouting.Builder builder = serviceBuilder.grpcRoutingBuilder();

        assertNotNull(builder, "GRPC routing builder should not be null");
    }

    /**
     * Tests the registerHttpService method with a single service.
     */
    @Test
    @DisplayName("registerHttpService should register a single HTTP service at the given path")
    void registerHttpService_withSingleService() {
        final String path = "/api/test";
        final HttpService mockService = mock(HttpService.class);

        // Create a spy on the internal HttpRouting.Builder to verify method calls
        final HttpRouting.Builder spyBuilder = spy(serviceBuilder.httpRoutingBuilder());

        // Use reflection to replace the builder with our spy
        try {
            final java.lang.reflect.Field field = ServiceBuilderImpl.class.getDeclaredField("httpRoutingBuilder");
            field.setAccessible(true);
            field.set(serviceBuilder, spyBuilder);
        } catch (Exception e) {
            fail("Failed to set up test: " + e.getMessage());
        }

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

        // Create a spy on the internal HttpRouting.Builder to verify method calls
        final HttpRouting.Builder spyBuilder = spy(serviceBuilder.httpRoutingBuilder());

        // Use reflection to replace the builder with our spy
        try {
            final java.lang.reflect.Field field = ServiceBuilderImpl.class.getDeclaredField("httpRoutingBuilder");
            field.setAccessible(true);
            field.set(serviceBuilder, spyBuilder);
        } catch (Exception e) {
            fail("Failed to set up test: " + e.getMessage());
        }

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

        // Create a spy on the internal HttpRouting.Builder to verify method calls
        final HttpRouting.Builder spyBuilder = spy(serviceBuilder.httpRoutingBuilder());

        // Use reflection to replace the builder with our spy
        try {
            final java.lang.reflect.Field field = ServiceBuilderImpl.class.getDeclaredField("httpRoutingBuilder");
            field.setAccessible(true);
            field.set(serviceBuilder, spyBuilder);
        } catch (Exception e) {
            fail("Failed to set up test: " + e.getMessage());
        }

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

        // Create a spy on the internal PbjRouting.Builder to verify method calls
        final PbjRouting.Builder spyBuilder = spy(serviceBuilder.grpcRoutingBuilder());

        // Use reflection to replace the builder with our spy
        try {
            final java.lang.reflect.Field field = ServiceBuilderImpl.class.getDeclaredField("pbjRoutingBuilder");
            field.setAccessible(true);
            field.set(serviceBuilder, spyBuilder);
        } catch (Exception e) {
            fail("Failed to set up test: " + e.getMessage());
        }

        serviceBuilder.registerGrpcService(mockService);

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
                () -> serviceBuilder.registerGrpcService(null),
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

        // Create a spy on the internal HttpRouting.Builder to verify method calls
        final HttpRouting.Builder spyBuilder = spy(serviceBuilder.httpRoutingBuilder());

        // Use reflection to replace the builder with our spy
        try {
            final java.lang.reflect.Field field = ServiceBuilderImpl.class.getDeclaredField("httpRoutingBuilder");
            field.setAccessible(true);
            field.set(serviceBuilder, spyBuilder);
        } catch (Exception e) {
            fail("Failed to set up test: " + e.getMessage());
        }

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

        // Create a spy on the internal PbjRouting.Builder to verify method calls
        final PbjRouting.Builder spyBuilder = spy(serviceBuilder.grpcRoutingBuilder());

        // Use reflection to replace the builder with our spy
        try {
            final java.lang.reflect.Field field = ServiceBuilderImpl.class.getDeclaredField("pbjRoutingBuilder");
            field.setAccessible(true);
            field.set(serviceBuilder, spyBuilder);
        } catch (Exception e) {
            fail("Failed to set up test: " + e.getMessage());
        }

        serviceBuilder.registerGrpcService(mockService1);
        serviceBuilder.registerGrpcService(mockService2);

        verify(spyBuilder).service(eq(mockService1));
        verify(spyBuilder).service(eq(mockService2));
    }
}
