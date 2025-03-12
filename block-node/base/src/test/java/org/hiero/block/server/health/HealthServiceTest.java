// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.health;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import org.hiero.block.server.service.ServiceStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class HealthServiceTest {

    private static final String READINESS_PATH = "/readyz";
    private static final String LIVENESS_PATH = "/livez";
    private static final String HEALTH_PATH = "/healthz";

    @Mock
    private ServiceStatus serviceStatus;

    @Mock
    ServerRequest serverRequest;

    @Mock
    ServerResponse serverResponse;

    @Test
    public void testHandleLivez() {
        // given
        when(serviceStatus.isRunning()).thenReturn(true);
        when(serverResponse.status(200)).thenReturn(serverResponse);
        doNothing().when(serverResponse).send("OK");
        HealthService healthService = new HealthServiceImpl(serviceStatus);

        // when
        healthService.handleLivez(serverRequest, serverResponse);

        // then
        verify(serverResponse, times(1)).status(200);
        verify(serverResponse, times(1)).send("OK");
    }

    @Test
    public void testHandleLivez_notRunning() {
        // given
        when(serviceStatus.isRunning()).thenReturn(false);
        when(serverResponse.status(503)).thenReturn(serverResponse);
        doNothing().when(serverResponse).send("Service is not running");
        HealthService healthService = new HealthServiceImpl(serviceStatus);

        // when
        healthService.handleLivez(serverRequest, serverResponse);

        // then
        verify(serverResponse, times(1)).status(503);
        verify(serverResponse, times(1)).send("Service is not running");
    }

    @Test
    public void testHandleReadyz() {
        // given
        when(serviceStatus.isRunning()).thenReturn(true);
        when(serverResponse.status(200)).thenReturn(serverResponse);
        doNothing().when(serverResponse).send("OK");
        HealthService healthService = new HealthServiceImpl(serviceStatus);

        // when
        healthService.handleReadyz(serverRequest, serverResponse);

        // then
        verify(serverResponse, times(1)).status(200);
        verify(serverResponse, times(1)).send("OK");
    }

    @Test
    public void testHandleReadyz_notRunning() {
        // given
        when(serviceStatus.isRunning()).thenReturn(false);
        when(serverResponse.status(503)).thenReturn(serverResponse);
        doNothing().when(serverResponse).send("Service is not running");
        HealthService healthService = new HealthServiceImpl(serviceStatus);

        // when
        healthService.handleReadyz(serverRequest, serverResponse);

        // then
        verify(serverResponse, times(1)).status(503);
        verify(serverResponse, times(1)).send("Service is not running");
    }

    @Test
    public void testRouting() {
        // given
        HealthService healthService = new HealthServiceImpl(serviceStatus);
        HttpRules httpRules = mock(HttpRules.class);
        when(httpRules.get(anyString(), any())).thenReturn(httpRules);

        // when
        healthService.routing(httpRules);

        // then
        verify(httpRules, times(1)).get(eq(LIVENESS_PATH), any());
        verify(httpRules, times(1)).get(eq(READINESS_PATH), any());
        assertEquals(HEALTH_PATH, healthService.getHealthRootPath());
    }
}
