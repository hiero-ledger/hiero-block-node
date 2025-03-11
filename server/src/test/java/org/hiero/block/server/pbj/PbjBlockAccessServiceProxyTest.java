// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.pbj;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.BlockUnparsed;
import com.hedera.hapi.block.SingleBlockRequest;
import com.hedera.hapi.block.SingleBlockResponse;
import com.hedera.hapi.block.SingleBlockResponseCode;
import com.hedera.hapi.block.SingleBlockResponseUnparsed;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.hiero.block.server.metrics.MetricsService;
import org.hiero.block.server.persistence.storage.read.BlockReader;
import org.hiero.block.server.service.ServiceStatus;
import org.hiero.block.server.util.TestConfigUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class PbjBlockAccessServiceProxyTest {

    @Mock
    private ServiceStatus serviceStatus;

    @Mock
    private BlockReader<BlockUnparsed> blockReader;

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
    public void testOpenWithIncorrectMethod() {

        final PbjBlockAccessServiceProxy pbjBlockAccessServiceProxy =
                new PbjBlockAccessServiceProxy(serviceStatus, blockReader, metricsService);
        Pipeline<? super Bytes> pipeline = pbjBlockAccessServiceProxy.open(
                PbjBlockStreamService.BlockStreamMethod.publishBlockStream, options, replies);

        verify(replies, timeout(testTimeout).times(1)).onError(any());
        assertNotNull(pipeline);
    }

    @Test
    public void testSingleBlock() throws IOException, ParseException {
        final PbjBlockAccessServiceProxy pbjBlockAccessServiceProxy =
                new PbjBlockAccessServiceProxy(serviceStatus, blockReader, metricsService);
        final Pipeline<? super Bytes> pipeline =
                pbjBlockAccessServiceProxy.open(PbjBlockAccessService.BlockAccessMethod.singleBlock, options, replies);
        assertNotNull(pipeline);

        when(serviceStatus.isRunning()).thenReturn(true);

        final var blockItems = BlockItemUnparsed.newBuilder()
                .blockHeader(BlockHeader.PROTOBUF.toBytes(
                        BlockHeader.newBuilder().number(1).build()))
                .build();
        final BlockUnparsed block =
                BlockUnparsed.newBuilder().blockItems(blockItems).build();
        when(blockReader.read(1)).thenReturn(Optional.of(block));

        final SingleBlockRequest singleBlockRequest =
                SingleBlockRequest.newBuilder().blockNumber(1).build();
        pipeline.onNext(SingleBlockRequest.PROTOBUF.toBytes(singleBlockRequest));

        final var readSuccessResponse = SingleBlockResponseUnparsed.newBuilder()
                .status(SingleBlockResponseCode.READ_BLOCK_SUCCESS)
                .block(block)
                .build();
        verify(replies, timeout(testTimeout).times(1)).onSubscribe(any());
        verify(replies, timeout(testTimeout).times(1))
                .onNext(SingleBlockResponseUnparsed.PROTOBUF.toBytes(readSuccessResponse));
        verify(replies, timeout(testTimeout).times(1)).onComplete();
    }

    @Test
    public void testSingleBlockNotFound() throws IOException, ParseException {
        final PbjBlockAccessServiceProxy pbjBlockAccessServiceProxy =
                new PbjBlockAccessServiceProxy(serviceStatus, blockReader, metricsService);
        final Pipeline<? super Bytes> pipeline =
                pbjBlockAccessServiceProxy.open(PbjBlockAccessService.BlockAccessMethod.singleBlock, options, replies);
        assertNotNull(pipeline);

        when(serviceStatus.isRunning()).thenReturn(true);
        when(blockReader.read(1)).thenReturn(Optional.empty());

        final SingleBlockRequest singleBlockRequest =
                SingleBlockRequest.newBuilder().blockNumber(1).build();
        pipeline.onNext(SingleBlockRequest.PROTOBUF.toBytes(singleBlockRequest));

        final var blockNotFound = SingleBlockResponse.newBuilder()
                .status(SingleBlockResponseCode.READ_BLOCK_NOT_FOUND)
                .build();
        verify(replies, timeout(testTimeout).times(1)).onSubscribe(any());
        verify(replies, timeout(testTimeout).times(1)).onNext(SingleBlockResponse.PROTOBUF.toBytes(blockNotFound));
        verify(replies, timeout(testTimeout).times(1)).onComplete();
    }

    @Test
    public void testSingleBlockIOException() throws IOException, ParseException {
        final PbjBlockAccessServiceProxy pbjBlockAccessServiceProxy =
                new PbjBlockAccessServiceProxy(serviceStatus, blockReader, metricsService);
        final Pipeline<? super Bytes> pipeline =
                pbjBlockAccessServiceProxy.open(PbjBlockAccessService.BlockAccessMethod.singleBlock, options, replies);
        assertNotNull(pipeline);

        when(serviceStatus.isRunning()).thenReturn(true);
        when(blockReader.read(1)).thenThrow(new IOException("Test IOException"));

        final SingleBlockRequest singleBlockRequest =
                SingleBlockRequest.newBuilder().blockNumber(1).build();
        pipeline.onNext(SingleBlockRequest.PROTOBUF.toBytes(singleBlockRequest));

        final var blockNotAvailable = SingleBlockResponse.newBuilder()
                .status(SingleBlockResponseCode.READ_BLOCK_NOT_AVAILABLE)
                .build();
        verify(replies, timeout(testTimeout).times(1)).onSubscribe(any());
        verify(replies, timeout(testTimeout).times(1)).onNext(SingleBlockResponse.PROTOBUF.toBytes(blockNotAvailable));
        verify(replies, timeout(testTimeout).times(1)).onComplete();
    }
}
