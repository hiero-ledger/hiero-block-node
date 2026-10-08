// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.swirlds.config.api.ConfigurationBuilder;
import io.helidon.common.socket.SocketOptions;
import java.util.Optional;
import org.hiero.block.node.app.config.ServerConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Tests for [BlockNodeApp#buildSocketOptions(ServerConfig)].
class BlockNodeAppSocketOptionsTest {
    private static final int EXPLICIT_BUFFER_BYTES = 131_072;

    @Test
    @DisplayName("a zero send buffer size leaves it unset so the kernel autotunes it")
    void buildSocketOptions_zeroSendBuffer_leavesSendBufferUnset() {
        final SocketOptions socketOptions = BlockNodeApp.buildSocketOptions(serverConfig("0", "0"));

        assertTrue(socketOptions.socketSendBufferSize().isEmpty());
    }

    @Test
    @DisplayName("a zero receive buffer size leaves it unset so the kernel autotunes it")
    void buildSocketOptions_zeroReceiveBuffer_leavesReceiveBufferUnset() {
        final SocketOptions socketOptions = BlockNodeApp.buildSocketOptions(serverConfig("0", "0"));

        assertTrue(socketOptions.socketReceiveBufferSize().isEmpty());
    }

    @Test
    @DisplayName("an explicit send buffer size is applied")
    void buildSocketOptions_explicitSendBuffer_setsSendBuffer() {
        final SocketOptions socketOptions =
                BlockNodeApp.buildSocketOptions(serverConfig(String.valueOf(EXPLICIT_BUFFER_BYTES), "0"));

        assertEquals(Optional.of(EXPLICIT_BUFFER_BYTES), socketOptions.socketSendBufferSize());
    }

    @Test
    @DisplayName("an explicit receive buffer size is applied")
    void buildSocketOptions_explicitReceiveBuffer_setsReceiveBuffer() {
        final SocketOptions socketOptions =
                BlockNodeApp.buildSocketOptions(serverConfig("0", String.valueOf(EXPLICIT_BUFFER_BYTES)));

        assertEquals(Optional.of(EXPLICIT_BUFFER_BYTES), socketOptions.socketReceiveBufferSize());
    }

    private static ServerConfig serverConfig(final String sendBufferBytes, final String receiveBufferBytes) {
        return ConfigurationBuilder.create()
                .autoDiscoverExtensions()
                .withConfigDataType(ServerConfig.class)
                .withValue("server.socketSendBufferSizeBytes", sendBufferBytes)
                .withValue("server.socketReceiveBufferSizeBytes", receiveBufferBytes)
                .build()
                .getConfigData(ServerConfig.class);
    }
}
