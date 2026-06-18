// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import org.hiero.block.api.NetworkConnection;
import org.hiero.block.api.NetworkConnection.IpProtocol;
import org.hiero.block.api.NetworkData;
import org.hiero.block.internal.BlockNodeSource;
import org.hiero.block.internal.BlockNodeSourceConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link BackfillNetworkData} conversion of backfill sources to {@link NetworkData}. */
class BackfillNetworkDataTest {

    @Test
    @DisplayName("Source maps to remote endpoint, wildcard self local, partner category, and empty certificate")
    void mapsSourceToConnection() {
        final BlockNodeSourceConfig node = BlockNodeSourceConfig.newBuilder()
                .address("source.example.com")
                .port(40840)
                .scheme("grpc")
                .protocol(IpProtocol.UDP)
                .build();

        final NetworkConnection connection = BackfillNetworkData.toNetworkConnection(node, true);

        assertEquals("source.example.com", connection.remote().address());
        assertEquals("40840", connection.remote().port());
        assertEquals("$", connection.local().address());
        assertEquals("*", connection.local().port());
        assertEquals("partner", connection.category());
        assertEquals("grpc", connection.scheme());
        assertEquals(IpProtocol.UDP, connection.protocol());
        assertTrue(connection.tlsRequired());
        assertEquals(Bytes.EMPTY, connection.certificate());
    }

    @Test
    @DisplayName("Blank scheme and unidentified protocol fall back to https/TCP when TLS is enabled")
    void defaultsWithTls() {
        final BlockNodeSourceConfig node =
                BlockNodeSourceConfig.newBuilder().address("a").port(1).build();

        final NetworkConnection connection = BackfillNetworkData.toNetworkConnection(node, true);

        assertEquals("https", connection.scheme());
        assertEquals(IpProtocol.TCP, connection.protocol());
        assertTrue(connection.tlsRequired());
    }

    @Test
    @DisplayName("Blank scheme falls back to http when TLS is disabled")
    void defaultsWithoutTls() {
        final BlockNodeSourceConfig node =
                BlockNodeSourceConfig.newBuilder().address("a").port(1).build();

        final NetworkConnection connection = BackfillNetworkData.toNetworkConnection(node, false);

        assertEquals("http", connection.scheme());
        assertFalse(connection.tlsRequired());
    }

    @Test
    @DisplayName("toNetworkData converts every configured source")
    void convertsAllSources() {
        final BlockNodeSource sources = BlockNodeSource.newBuilder()
                .nodes(List.of(
                        BlockNodeSourceConfig.newBuilder().address("a").port(1).build(),
                        BlockNodeSourceConfig.newBuilder().address("b").port(2).build()))
                .build();

        final NetworkData data = BackfillNetworkData.toNetworkData(sources, false);

        assertEquals(2, data.activeEndpoints().size());
        assertEquals("a", data.activeEndpoints().get(0).remote().address());
        assertEquals("b", data.activeEndpoints().get(1).remote().address());
    }
}
