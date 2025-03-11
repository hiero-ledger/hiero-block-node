// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.pbj;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.hedera.block.server.metrics.MetricsService;
import com.hedera.block.server.persistence.storage.read.BlockReader;
import com.hedera.block.server.service.ServiceStatus;
import com.hedera.block.server.util.TestConfigUtil;
import com.hedera.hapi.block.BlockNodeVersions;
import com.hedera.hapi.block.ServerStatusRequest;
import com.hedera.hapi.block.ServerStatusResponse;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class PbjServerStatusServiceProxyTest {

    @Mock
    private ServiceStatus serviceStatus;

    @Mock
    private BlockReader blockReader;

    @Mock
    private ServiceInterface.RequestOptions options;

    @Mock
    private Pipeline<? super Bytes> replies;

    private MetricsService metricsService;

    private static final int testTimeout = 100;

    @BeforeEach
    public void setUp() throws IOException {
        Map<String, String> properties = new HashMap<>();
        metricsService = TestConfigUtil.getTestBlockNodeMetricsService(properties);
    }

    @Test
    public void testServerStatus() {
        final PbjServerStatusServiceProxy serviceProxy = new PbjServerStatusServiceProxy(serviceStatus, metricsService);

        Pipeline<? super Bytes> pipeline =
                serviceProxy.open(PbjServerStatusService.ServerStatusMethod.serverStatus, options, replies);
        assertNotNull(pipeline);

        when(serviceStatus.isRunning()).thenReturn(true);
        when(serviceStatus.getFirstAvailableBlockNumber()).thenReturn(0L);
        when(serviceStatus.getLatestReceivedBlockNumber()).thenReturn(100L);
        when(serviceStatus.getOnlyLatestState()).thenReturn(false);

        // Create a real BlockNodeVersions instance
        final BlockNodeVersions versionInfo = new BlockNodeVersions(
                new SemanticVersion(0, 1, 0, "", ""),
                new SemanticVersion(0, 1, 0, "", ""),
                new SemanticVersion(0, 4, 0, "", ""));
        when(serviceStatus.getVersionInformation()).thenReturn(versionInfo);

        final ServerStatusRequest request = ServerStatusRequest.newBuilder().build();
        pipeline.onNext(ServerStatusRequest.PROTOBUF.toBytes(request));

        final ServerStatusResponse expectedResponse = ServerStatusResponse.newBuilder()
                .firstAvailableBlock(0)
                .lastAvailableBlock(100)
                .onlyLatestState(false)
                .versionInformation(versionInfo)
                .build();

        verify(replies, timeout(testTimeout).times(1)).onSubscribe(any());
        verify(replies, timeout(testTimeout).times(1)).onNext(ServerStatusResponse.PROTOBUF.toBytes(expectedResponse));
        verify(replies, timeout(testTimeout).times(1)).onComplete();
    }

    @Test
    public void testServerStatusWhenNotRunning() {
        final PbjServerStatusServiceProxy serviceProxy = new PbjServerStatusServiceProxy(serviceStatus, metricsService);

        Pipeline<? super Bytes> pipeline =
                serviceProxy.open(PbjServerStatusService.ServerStatusMethod.serverStatus, options, replies);
        assertNotNull(pipeline);

        when(serviceStatus.isRunning()).thenReturn(false);

        final ServerStatusRequest request = ServerStatusRequest.newBuilder().build();
        pipeline.onNext(ServerStatusRequest.PROTOBUF.toBytes(request));

        final ServerStatusResponse expectedResponse =
                ServerStatusResponse.newBuilder().build();

        verify(replies, timeout(testTimeout).times(1)).onSubscribe(any());
        verify(replies, timeout(testTimeout).times(1)).onNext(ServerStatusResponse.PROTOBUF.toBytes(expectedResponse));
        verify(replies, timeout(testTimeout).times(1)).onComplete();
    }
}
