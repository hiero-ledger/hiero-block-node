// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import java.util.stream.Stream;
import org.hiero.block.node.app.config.ServerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
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

    @Test
    @DisplayName("Default publisher port is 40840")
    void defaultPortIs40840() {
        assertEquals(40840, defaultConfig().getConfigData(ServerConfig.class).port());
    }

    @Test
    @DisplayName("Default consumer port is 40940")
    void defaultConsumerPortIs40940() {
        assertEquals(40940, defaultConfig().getConfigData(ServerConfig.class).consumerPort());
    }

    @Test
    @DisplayName("Default ports are distinct (two-port mode by default)")
    void defaultPortsAreDistinct() {
        final ServerConfig cfg = defaultConfig().getConfigData(ServerConfig.class);
        assertNotEquals(cfg.port(), cfg.consumerPort(), "port and consumerPort should differ in the default config");
    }

    @Test
    @DisplayName("consumerPort can be set equal to port for single-port mode")
    void consumerPortCanMatchPort() {
        final Configuration config = ConfigurationBuilder.create()
                .autoDiscoverExtensions()
                .withConfigDataType(ServerConfig.class)
                .withValue("server.consumerPort", "40840")
                .build();
        final ServerConfig cfg = config.getConfigData(ServerConfig.class);
        assertEquals(cfg.port(), cfg.consumerPort());
    }

    @Test
    @DisplayName("consumerPort can be overridden to an arbitrary valid value")
    void consumerPortCanBeOverridden() {
        final Configuration config = ConfigurationBuilder.create()
                .autoDiscoverExtensions()
                .withConfigDataType(ServerConfig.class)
                .withValue("server.consumerPort", "8443")
                .build();
        assertEquals(8443, config.getConfigData(ServerConfig.class).consumerPort());
    }

    @Test
    @DisplayName("publisher port can be overridden independently of consumer port")
    void publisherPortCanBeOverriddenIndependently() {
        final Configuration config = ConfigurationBuilder.create()
                .autoDiscoverExtensions()
                .withConfigDataType(ServerConfig.class)
                .withValue("server.port", "12345")
                .build();
        final ServerConfig cfg = config.getConfigData(ServerConfig.class);
        assertEquals(12345, cfg.port());
        assertEquals(40940, cfg.consumerPort()); // consumer port unchanged
    }

    private static Configuration defaultConfig() {
        return ConfigurationBuilder.create()
                .autoDiscoverExtensions()
                .withConfigDataType(ServerConfig.class)
                .build();
    }

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
                        String.format(RANGE_ERROR_TEMPLATE, "server.maxMessageSizeBytes", 10_238, 10_240, 37_748_736)),
                Arguments.of(
                        10_239,
                        String.format(RANGE_ERROR_TEMPLATE, "server.maxMessageSizeBytes", 10_239, 10_240, 37_748_736)),
                Arguments.of(
                        37_748_737,
                        String.format(
                                RANGE_ERROR_TEMPLATE, "server.maxMessageSizeBytes", 37_748_737, 10_240, 37_748_736)));
    }
}
