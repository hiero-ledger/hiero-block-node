// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.pbj;

import static org.hiero.block.server.pbj.TestUtils.buildSubscribeStreamRequest;
import static org.hiero.block.server.service.Constants.READ_STREAM_INVALID_END_BLOCK_NUMBER_RESPONSE;
import static org.hiero.block.server.service.Constants.READ_STREAM_INVALID_START_BLOCK_NUMBER_RESPONSE;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.hedera.hapi.block.SubscribeStreamResponseUnparsed;
import com.hedera.pbj.runtime.grpc.Pipeline;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class CurrentBlockNumberRangeTest {

    @Mock
    private Pipeline<SubscribeStreamResponseUnparsed> helidonConsumerObserver;

    @ParameterizedTest
    @MethodSource("outOfRangeBlockNumbers")
    public void testInvalidBlockNumbers(
            long startBlockNumber,
            long endBlockNumber,
            long currentBlockNumber,
            final SubscribeStreamResponseUnparsed expectedResponse) {
        assertFalse(PbjBlockStreamServiceProxy.isRequestedRangeValidForCurrentBlock(
                buildSubscribeStreamRequest(startBlockNumber, endBlockNumber),
                currentBlockNumber,
                helidonConsumerObserver));
        verify(helidonConsumerObserver, times(1)).onNext(expectedResponse);
        verify(helidonConsumerObserver, times(1)).onComplete();
    }

    @ParameterizedTest
    @MethodSource("inRangeBlockNumbers")
    public void testValidBlockNumbers(long startBlockNumber, long endBlockNumber, long currentBlockNumber) {
        assertTrue(PbjBlockStreamServiceProxy.isRequestedRangeValidForCurrentBlock(
                buildSubscribeStreamRequest(startBlockNumber, endBlockNumber),
                currentBlockNumber,
                helidonConsumerObserver));
        verify(helidonConsumerObserver, never()).onNext(any());
    }

    private static Stream<Arguments> outOfRangeBlockNumbers() {
        return Stream.of(
                Arguments.of(5, 10, 1, READ_STREAM_INVALID_START_BLOCK_NUMBER_RESPONSE),
                Arguments.of(5, 10, 7, READ_STREAM_INVALID_END_BLOCK_NUMBER_RESPONSE));
    }

    private static Stream<Arguments> inRangeBlockNumbers() {
        return Stream.of(Arguments.of(5, 10, 15), Arguments.of(5, 10, 10));
    }
}
