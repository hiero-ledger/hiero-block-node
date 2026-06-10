// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.health;

import static org.hiero.block.node.health.HealthServicePlugin.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.swirlds.config.api.Configuration;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.health.HealthFacility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class HealthServiceTest {

    @Mock
    ServerRequest serverRequest;

    @Mock
    ServerResponse serverResponse;

    @Mock
    ServiceBuilder serviceBuilder;

    /** Stub the context configuration so {@code init} can read {@link HealthConfig} (port unset = null). */
    private static void setupContextConfig(BlockNodeContext context) {
        final Configuration configuration = Mockito.mock(Configuration.class);
        Mockito.when(configuration.getConfigData(HealthConfig.class)).thenReturn(new HealthConfig(null));
        Mockito.when(context.configuration()).thenReturn(configuration);
    }

    @Test
    public void testHandleLivez() {
        // given
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
        Mockito.verify(serverResponse, Mockito.times(1)).status(200);
        Mockito.verify(serverResponse, Mockito.times(1)).send("OK");
    }

    @Test
    public void testHandleLivez_notRunning() {
        // given
        Mockito.when(serverResponse.status(503)).thenReturn(serverResponse);

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
        Mockito.verify(serverResponse, Mockito.times(1)).send("Service is not running");
    }

    @Test
    public void testHandleReadyz() {
        // given
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
        healthServicePlugin.handleReadyz(serverRequest, serverResponse);

        // then
        Mockito.verify(serverResponse, Mockito.times(1)).status(200);
        Mockito.verify(serverResponse, Mockito.times(1)).send("OK");
    }

    @Test
    public void testHandleReadyz_notRunning() {
        // given
        Mockito.when(serverResponse.status(503)).thenReturn(serverResponse);

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
        Mockito.verify(serverResponse, Mockito.times(1)).send("Service is not running");
    }

    @Test
    public void testRouting() {
        // given
        ArgumentCaptor<HttpService> httpServiceArgumentCaptor = ArgumentCaptor.forClass(HttpService.class);
        HttpRules httpRules = Mockito.mock(HttpRules.class);
        Mockito.when(httpRules.get(ArgumentMatchers.anyString(), ArgumentMatchers.any()))
                .thenReturn(httpRules);
        BlockNodeContext context = Mockito.mock(BlockNodeContext.class);
        setupContextConfig(context);

        // when
        HealthServicePlugin healthServicePlugin = new HealthServicePlugin();
        healthServicePlugin.init(context, serviceBuilder);

        // then
        Mockito.verify(serviceBuilder, Mockito.times(1))
                .registerHttpService(
                        ArgumentMatchers.eq(HEALTHZ_PATH),
                        ArgumentMatchers.isNull(),
                        httpServiceArgumentCaptor.capture());
        HttpService httpService = httpServiceArgumentCaptor.getValue();
        assertNotNull(httpService);

        // healthServicePlugin used a functionalInterface to define HttpService so we have to confirm
        // healthServicePlugin.routing() will apply the expected routing paths
        // confirm that httRules.get(..) is called twice, once for READINESS_PATH and then once for LIVENESS_PATH
        httpService.routing(httpRules);
        Mockito.verify(httpRules, Mockito.times(1)).get(ArgumentMatchers.eq(READYZ_PATH), ArgumentMatchers.any());
        Mockito.verify(httpRules, Mockito.times(1)).get(ArgumentMatchers.eq(LIVEZ_PATH), ArgumentMatchers.any());
    }
}
