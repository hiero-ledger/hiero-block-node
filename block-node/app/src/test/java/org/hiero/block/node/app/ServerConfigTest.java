// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.provider.Arguments;

@SuppressWarnings("unused")
class ServerConfigTest {

    private static final String RANGE_ERROR_TEMPLATE = "%s value %d is out of range [%d, %d]";

    @BeforeEach
    void setUp() {}

    @AfterEach
    void tearDown() {}
    //
    //    @Test
    //    void testValidValues() {
    //        ServerConfig serverConfig = new ServerConfig(4_194_304, 32_768, 32_768, 8080);
    //        assertEquals(4_194_304, serverConfig.maxMessageSizeBytes());
    //        assertEquals(8080, serverConfig.port());
    //    }
    //
    //    @ParameterizedTest
    //    @MethodSource("outOfRangeMaxMessageSizes")
    //    void testMessageSizesOutOfBounds(final int messageSize, final String message) {
    //        assertThatIllegalArgumentException()
    //                .isThrownBy(() -> new ServerConfig(messageSize, 32_768, 32_768, 8080))
    //                .withMessage(message);
    //    }
    //
    //    @ParameterizedTest
    //    @MethodSource("outOfRangeSendBufferSizes")
    //    void testSocketSendBufferSize(int sendBufferSize, String message) {
    //        assertThatIllegalArgumentException()
    //                .isThrownBy(() -> new ServerConfig(4_194_304, sendBufferSize, 32_768, 8080))
    //                .withMessage(message);
    //    }
    //
    //    @ParameterizedTest
    //    @MethodSource("outOfRangeReceiveBufferSizes")
    //    void testSocketReceiveBufferSize(int receiveBufferSize, String message) {
    //        assertThatIllegalArgumentException()
    //                .isThrownBy(() -> new ServerConfig(4_194_304, 32_768, receiveBufferSize, 8080))
    //                .withMessage(message);
    //    }
    //
    //    @ParameterizedTest
    //    @MethodSource("outOfRangePorts")
    //    void testPortValues(final int port, final String message) {
    //        assertThatIllegalArgumentException()
    //                .isThrownBy(() -> new ServerConfig(4_194_304, 32_768, 32_768, port))
    //                .withMessage(message);
    //    }

    private static Stream<Arguments> outOfRangePorts() {
        return Stream.of(
                Arguments.of(1023, String.format(RANGE_ERROR_TEMPLATE, "server.port", 1023, 1024, 65_535)),
                Arguments.of(65_536, String.format(RANGE_ERROR_TEMPLATE, "server.port", 65_536, 1024, 65_535)));
    }

    private static Stream<Arguments> outOfRangeReceiveBufferSizes() {
        return Stream.of(
                Arguments.of(
                        32_767,
                        String.format(
                                RANGE_ERROR_TEMPLATE,
                                "server.socketReceiveBufferSizeBytes",
                                32_767,
                                32768,
                                Integer.MAX_VALUE)),
                Arguments.of(
                        1,
                        String.format(
                                RANGE_ERROR_TEMPLATE,
                                "server.socketReceiveBufferSizeBytes",
                                1,
                                32768,
                                Integer.MAX_VALUE)));
    }

    private static Stream<Arguments> outOfRangeSendBufferSizes() {
        return Stream.of(
                Arguments.of(
                        32_767,
                        String.format(
                                RANGE_ERROR_TEMPLATE,
                                "server.socketSendBufferSizeBytes",
                                32_767,
                                32768,
                                Integer.MAX_VALUE)),
                Arguments.of(
                        1,
                        String.format(
                                RANGE_ERROR_TEMPLATE,
                                "server.socketSendBufferSizeBytes",
                                1,
                                32768,
                                Integer.MAX_VALUE)));
    }

    private static Stream<Arguments> outOfRangeMaxMessageSizes() {
        return Stream.of(
                Arguments.of(
                        10_238,
                        String.format(RANGE_ERROR_TEMPLATE, "server.maxMessageSizeBytes", 10_238, 10_240, 16_777_215)),
                Arguments.of(
                        10_239,
                        String.format(RANGE_ERROR_TEMPLATE, "server.maxMessageSizeBytes", 10_239, 10_240, 16_777_215)),
                Arguments.of(
                        16_777_216,
                        String.format(
                                RANGE_ERROR_TEMPLATE, "server.maxMessageSizeBytes", 16_777_216, 10_240, 16_777_215)));
    }
}
