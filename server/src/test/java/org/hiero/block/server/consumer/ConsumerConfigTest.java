// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.consumer;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.hiero.block.server.consumer.ConsumerConfig.minMaxBlockItemBatchSize;
import static org.hiero.block.server.consumer.ConsumerConfig.minTimeoutThresholdMillis;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ConsumerConfigTest {

    @ParameterizedTest
    @MethodSource("outOfRangeMaxBlockItemBatchSize")
    public void testMaxBlockItemBatchSize(int maxBlockItemBatchSize, final String message) {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new ConsumerConfig(1500, 3, maxBlockItemBatchSize))
                .withMessage(message);
    }

    @ParameterizedTest
    @MethodSource("outOfRangeTimeoutThresholdMillis")
    public void testTimeoutThresholdMillis(int timeoutThresholdMillis, final String message) {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new ConsumerConfig(timeoutThresholdMillis, 3, 1000))
                .withMessage(message);
    }

    @ParameterizedTest
    @MethodSource("outOfRangeCueHistoricStreamingPaddingBlocks")
    public void testCueHistoricStreamingPaddingBlocks(int cueHistoricStreamingPaddingBlocks, final String message) {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new ConsumerConfig(1500, cueHistoricStreamingPaddingBlocks, 1000))
                .withMessage(message);
    }

    private static Stream<Arguments> outOfRangeMaxBlockItemBatchSize() {
        return Stream.of(
                Arguments.of(
                        0,
                        String.format(
                                "The input number [%d] is required to be greater or equal than [%d].",
                                0, minMaxBlockItemBatchSize)),
                Arguments.of(
                        -1,
                        String.format(
                                "The input number [%d] is required to be greater or equal than [%d].",
                                -1, minMaxBlockItemBatchSize)));
    }

    private static Stream<Arguments> outOfRangeTimeoutThresholdMillis() {
        return Stream.of(
                Arguments.of(
                        -1,
                        String.format(
                                "The input number [%d] is required to be greater or equal than [%d].",
                                -1, minTimeoutThresholdMillis)),
                Arguments.of(
                        0,
                        String.format(
                                "The input number [%d] is required to be greater or equal than [%d].",
                                0, minTimeoutThresholdMillis)));
    }

    private static Stream<Arguments> outOfRangeCueHistoricStreamingPaddingBlocks() {
        return Stream.of(
                Arguments.of(
                        0,
                        String.format(
                                "The input number [%d] is required to be greater or equal than [%d].",
                                0, minMaxBlockItemBatchSize)),
                Arguments.of(
                        -1,
                        String.format(
                                "The input number [%d] is required to be greater or equal than [%d].",
                                -1, minMaxBlockItemBatchSize)));
    }
}
