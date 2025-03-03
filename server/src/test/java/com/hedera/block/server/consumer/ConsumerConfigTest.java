// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.consumer;

import static com.hedera.block.server.consumer.ConsumerConfig.minMaxBlockItemBatchSize;
import static com.hedera.block.server.consumer.ConsumerConfig.minTimeoutThresholdMillis;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ConsumerConfigTest {

    private static final String RANGE_ERROR_TEMPLATE = "%s value %d is out of range [%d, %d]";

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
                                RANGE_ERROR_TEMPLATE,
                                "consumer.maxBlockItemBatchSize",
                                0,
                                minMaxBlockItemBatchSize,
                                Integer.MAX_VALUE)),
                Arguments.of(
                        -1,
                        String.format(
                                RANGE_ERROR_TEMPLATE,
                                "consumer.maxBlockItemBatchSize",
                                -1,
                                minMaxBlockItemBatchSize,
                                Integer.MAX_VALUE)));
    }

    private static Stream<Arguments> outOfRangeTimeoutThresholdMillis() {
        return Stream.of(
                Arguments.of(
                        -1,
                        String.format(
                                RANGE_ERROR_TEMPLATE,
                                "consumer.timeoutThresholdMillis",
                                -1,
                                minTimeoutThresholdMillis,
                                Integer.MAX_VALUE)),
                Arguments.of(
                        0,
                        String.format(
                                RANGE_ERROR_TEMPLATE,
                                "consumer.timeoutThresholdMillis",
                                0,
                                minTimeoutThresholdMillis,
                                Integer.MAX_VALUE)));
    }

    private static Stream<Arguments> outOfRangeCueHistoricStreamingPaddingBlocks() {
        return Stream.of(
                Arguments.of(
                        0,
                        String.format(
                                RANGE_ERROR_TEMPLATE,
                                "consumer.cueHistoricStreamingPaddingBlocks",
                                0,
                                minMaxBlockItemBatchSize,
                                Integer.MAX_VALUE)),
                Arguments.of(
                        -1,
                        String.format(
                                RANGE_ERROR_TEMPLATE,
                                "consumer.cueHistoricStreamingPaddingBlocks",
                                -1,
                                minMaxBlockItemBatchSize,
                                Integer.MAX_VALUE)));
    }
}
