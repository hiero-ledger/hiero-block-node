// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hiero.block.server.Constants.FULL_SERVICE_NAME_BLOCK_ACCESS;
import static org.hiero.block.server.Constants.SERVICE_NAME_BLOCK_ACCESS;
import static org.hiero.block.server.util.PersistTestUtils.PERSISTENCE_STORAGE_LIVE_ROOT_PATH_KEY;
import static org.hiero.block.server.util.PersistTestUtils.generateBlockItemsUnparsedForWithBlockNumber;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.BlockUnparsed;
import com.hedera.hapi.block.SingleBlockRequest;
import com.hedera.hapi.block.SingleBlockResponseCode;
import com.hedera.hapi.block.SingleBlockResponseUnparsed;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hiero.block.server.config.BlockNodeContext;
import org.hiero.block.server.pbj.PbjBlockAccessService;
import org.hiero.block.server.pbj.PbjBlockAccessServiceProxy;
import org.hiero.block.server.persistence.storage.PersistenceStorageConfig;
import org.hiero.block.server.persistence.storage.read.BlockReader;
import org.hiero.block.server.service.ServiceStatus;
import org.hiero.block.server.util.TestConfigUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@SuppressWarnings("FieldCanBeLocal")
@ExtendWith(MockitoExtension.class)
class BlockAccessServiceTest {
    @Mock
    private Pipeline<? super Bytes> responseObserver;

    @Mock
    private BlockReader<BlockUnparsed> blockReader;

    @Mock
    private ServiceStatus serviceStatus;

    @TempDir
    private Path testLiveRootPath;

    private BlockNodeContext blockNodeContext;
    private PersistenceStorageConfig testConfig;
    private PbjBlockAccessService blockAccessService;

    @BeforeEach
    void setUp() throws IOException {
        blockNodeContext = TestConfigUtil.getTestBlockNodeContext(
                Map.of(PERSISTENCE_STORAGE_LIVE_ROOT_PATH_KEY, testLiveRootPath.toString()));
        testConfig = blockNodeContext.configuration().getConfigData(PersistenceStorageConfig.class);

        blockAccessService = new PbjBlockAccessServiceProxy(serviceStatus, blockReader, blockNodeContext);

        final Path testConfigLiveRootPath = testConfig.liveRootPath();
        assertThat(testConfigLiveRootPath).isEqualTo(testLiveRootPath);
    }

    @Test
    void testServiceName() {
        assertEquals(SERVICE_NAME_BLOCK_ACCESS, blockAccessService.serviceName());
    }

    @Test
    void testFullName() {
        assertEquals(FULL_SERVICE_NAME_BLOCK_ACCESS, blockAccessService.fullName());
    }

    @Test
    void testMethods() {
        assertEquals(1, blockAccessService.methods().size());
    }

    @Test
    void testSingleBlockHappyPath() throws IOException, ParseException {
        final long blockNumber = 1L;
        final List<BlockItemUnparsed> blockItems = generateBlockItemsUnparsedForWithBlockNumber(blockNumber);
        final BlockUnparsed targetBlock =
                BlockUnparsed.newBuilder().blockItems(blockItems).build();

        when(blockReader.read(blockNumber)).thenReturn(Optional.of(targetBlock));
        when(serviceStatus.isRunning()).thenReturn(true);

        // Build a response to verify what's passed to the response observer
        final SingleBlockResponseUnparsed expectedSingleBlockResponse = SingleBlockResponseUnparsed.newBuilder()
                .block(targetBlock)
                .status(SingleBlockResponseCode.READ_BLOCK_SUCCESS)
                .build();

        // Build a request to invoke the service
        final SingleBlockRequest singleBlockRequest =
                SingleBlockRequest.newBuilder().blockNumber(1).build();

        final Pipeline<? super Bytes> pipeline =
                blockAccessService.open(PbjBlockAccessService.BlockAccessMethod.singleBlock, null, responseObserver);

        // Call the service
        pipeline.onNext(SingleBlockRequest.PROTOBUF.toBytes(singleBlockRequest));
        verify(responseObserver, times(1))
                .onNext(SingleBlockResponseUnparsed.PROTOBUF.toBytes(expectedSingleBlockResponse));
    }

    @Test
    void testSingleBlockNotFoundPath() throws IOException, ParseException {
        // Get the block so we can verify the response payload
        when(blockReader.read(1)).thenReturn(Optional.empty());

        // Build a response to verify what's passed to the response observer
        final SingleBlockResponseUnparsed expectedNotFound = SingleBlockResponseUnparsed.newBuilder()
                .status(SingleBlockResponseCode.READ_BLOCK_NOT_FOUND)
                .build();

        // Build a request to invoke the service
        final SingleBlockRequest singleBlockRequest =
                SingleBlockRequest.newBuilder().blockNumber(1).build();

        // Enable the serviceStatus
        when(serviceStatus.isRunning()).thenReturn(true);

        final Pipeline<? super Bytes> pipeline =
                blockAccessService.open(PbjBlockAccessService.BlockAccessMethod.singleBlock, null, responseObserver);

        // Call the service
        pipeline.onNext(SingleBlockRequest.PROTOBUF.toBytes(singleBlockRequest));
        verify(responseObserver, times(1)).onNext(SingleBlockResponseUnparsed.PROTOBUF.toBytes(expectedNotFound));
    }

    @Test
    void testSingleBlockServiceNotAvailable() {
        // Set the service status to not running
        when(serviceStatus.isRunning()).thenReturn(false);

        final SingleBlockResponseUnparsed expectedNotAvailable = SingleBlockResponseUnparsed.newBuilder()
                .status(SingleBlockResponseCode.READ_BLOCK_NOT_AVAILABLE)
                .build();

        // Build a request to invoke the service
        final SingleBlockRequest singleBlockRequest =
                SingleBlockRequest.newBuilder().blockNumber(1).build();

        final Pipeline<? super Bytes> pipeline =
                blockAccessService.open(PbjBlockAccessService.BlockAccessMethod.singleBlock, null, responseObserver);

        // Call the service
        pipeline.onNext(SingleBlockRequest.PROTOBUF.toBytes(singleBlockRequest));
        verify(responseObserver, times(1)).onNext(SingleBlockResponseUnparsed.PROTOBUF.toBytes(expectedNotAvailable));
    }

    @Test
    void testSingleBlockIOExceptionPath() throws IOException, ParseException {
        when(serviceStatus.isRunning()).thenReturn(true);
        when(blockReader.read(1)).thenThrow(new IOException("Test exception"));

        final SingleBlockResponseUnparsed expectedNotAvailable = SingleBlockResponseUnparsed.newBuilder()
                .status(SingleBlockResponseCode.READ_BLOCK_NOT_AVAILABLE)
                .build();

        // Build a request to invoke the service
        final SingleBlockRequest singleBlockRequest =
                SingleBlockRequest.newBuilder().blockNumber(1).build();

        final Pipeline<? super Bytes> pipeline =
                blockAccessService.open(PbjBlockAccessService.BlockAccessMethod.singleBlock, null, responseObserver);

        // Call the service
        pipeline.onNext(SingleBlockRequest.PROTOBUF.toBytes(singleBlockRequest));
        verify(responseObserver, times(1)).onNext(SingleBlockResponseUnparsed.PROTOBUF.toBytes(expectedNotAvailable));
    }

    @Test
    void testSingleBlockParseExceptionPath() throws IOException, ParseException {
        when(serviceStatus.isRunning()).thenReturn(true);
        when(blockReader.read(1)).thenThrow(new ParseException("Test exception"));

        final SingleBlockResponseUnparsed expectedNotAvailable = SingleBlockResponseUnparsed.newBuilder()
                .status(SingleBlockResponseCode.READ_BLOCK_NOT_AVAILABLE)
                .build();

        // Build a request to invoke the service
        final SingleBlockRequest singleBlockRequest =
                SingleBlockRequest.newBuilder().blockNumber(1).build();

        final Pipeline<? super Bytes> pipeline =
                blockAccessService.open(PbjBlockAccessService.BlockAccessMethod.singleBlock, null, responseObserver);

        // Call the service
        pipeline.onNext(SingleBlockRequest.PROTOBUF.toBytes(singleBlockRequest));
        verify(responseObserver, times(1)).onNext(SingleBlockResponseUnparsed.PROTOBUF.toBytes(expectedNotAvailable));
    }
}
