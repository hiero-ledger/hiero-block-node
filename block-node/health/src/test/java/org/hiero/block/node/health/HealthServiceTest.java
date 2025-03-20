// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.health;

import static org.mockito.Mockito.*;

import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class HealthServiceTest {
    // TODO: Uncomment the following lines and fix the test cases
    //
    //    private static final String READINESS_PATH = "/readyz";
    //    private static final String LIVENESS_PATH = "/livez";
    //    private static final String HEALTH_PATH = "/healthz";
    //
    //    @Mock
    //    private ServiceStatus serviceStatus;
    //
    //    @Mock
    //    ServerRequest serverRequest;
    //
    //    @Mock
    //    ServerResponse serverResponse;
    //
    //    @Test
    //    public void testHandleLivez() {
    //        // given
    //        Mockito.when(serviceStatus.isRunning()).thenReturn(true);
    //        Mockito.when(serverResponse.status(200)).thenReturn(serverResponse);
    //        Mockito.doNothing().when(serverResponse).send("OK");
    //        HealthService healthService = new HealthServiceImpl(serviceStatus);
    //
    //        // when
    //        healthService.handleLivez(serverRequest, serverResponse);
    //
    //        // then
    //        Mockito.verify(serverResponse, Mockito.times(1)).status(200);
    //        Mockito.verify(serverResponse, Mockito.times(1)).send("OK");
    //    }
    //
    //    @Test
    //    public void testHandleLivez_notRunning() {
    //        // given
    //        Mockito.when(serviceStatus.isRunning()).thenReturn(false);
    //        Mockito.when(serverResponse.status(503)).thenReturn(serverResponse);
    //        Mockito.doNothing().when(serverResponse).send("Service is not running");
    //        HealthService healthService = new HealthServiceImpl(serviceStatus);
    //
    //        // when
    //        healthService.handleLivez(serverRequest, serverResponse);
    //
    //        // then
    //        Mockito.verify(serverResponse, Mockito.times(1)).status(503);
    //        Mockito.verify(serverResponse, Mockito.times(1)).send("Service is not running");
    //    }
    //
    //    @Test
    //    public void testHandleReadyz() {
    //        // given
    //        Mockito.when(serviceStatus.isRunning()).thenReturn(true);
    //        Mockito.when(serverResponse.status(200)).thenReturn(serverResponse);
    //        Mockito.doNothing().when(serverResponse).send("OK");
    //        HealthService healthService = new HealthServiceImpl(serviceStatus);
    //
    //        // when
    //        healthService.handleReadyz(serverRequest, serverResponse);
    //
    //        // then
    //        Mockito.verify(serverResponse, Mockito.times(1)).status(200);
    //        Mockito.verify(serverResponse, Mockito.times(1)).send("OK");
    //    }
    //
    //    @Test
    //    public void testHandleReadyz_notRunning() {
    //        // given
    //        Mockito.when(serviceStatus.isRunning()).thenReturn(false);
    //        Mockito.when(serverResponse.status(503)).thenReturn(serverResponse);
    //        Mockito.doNothing().when(serverResponse).send("Service is not running");
    //        HealthService healthService = new HealthServiceImpl(serviceStatus);
    //
    //        // when
    //        healthService.handleReadyz(serverRequest, serverResponse);
    //
    //        // then
    //        Mockito.verify(serverResponse, Mockito.times(1)).status(503);
    //        Mockito.verify(serverResponse, Mockito.times(1)).send("Service is not running");
    //    }
    //
    //    @Test
    //    public void testRouting() {
    //        // given
    //        HealthService healthService = new HealthServiceImpl(serviceStatus);
    //        HttpRules httpRules = Mockito.mock(HttpRules.class);
    //        Mockito.when(httpRules.get(ArgumentMatchers.anyString(), ArgumentMatchers.any())).thenReturn(httpRules);
    //
    //        // when
    //        healthService.routing(httpRules);
    //
    //        // then
    //        Mockito.verify(httpRules, Mockito.times(1)).get(ArgumentMatchers.eq(LIVENESS_PATH),
    // ArgumentMatchers.any());
    //        Mockito.verify(httpRules, Mockito.times(1)).get(ArgumentMatchers.eq(READINESS_PATH),
    // ArgumentMatchers.any());
    //        assertEquals(HEALTH_PATH, healthService.getHealthRootPath());
    //    }
}
