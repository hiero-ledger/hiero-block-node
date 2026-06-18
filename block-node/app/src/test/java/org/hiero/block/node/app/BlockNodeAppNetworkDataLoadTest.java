// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.hiero.block.api.NetworkConnection;
import org.hiero.block.api.NetworkConnection.ConnectionReference;
import org.hiero.block.api.NetworkConnection.IpProtocol;
import org.hiero.block.api.NetworkData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Unit tests for {@link BlockNodeApp#loadNetworkData(Path)}. */
class BlockNodeAppNetworkDataLoadTest {

    @Test
    @DisplayName("A valid NetworkData JSON file is parsed")
    void loadsNetworkDataFromFile(@TempDir final Path dir) throws IOException {
        final NetworkData data = NetworkData.newBuilder()
                .activeEndpoints(NetworkConnection.newBuilder()
                        .local(new ConnectionReference("$", "*"))
                        .remote(new ConnectionReference("pub.example.com", "40840"))
                        .category("publisher")
                        .scheme("https")
                        .protocol(IpProtocol.TCP)
                        .tlsRequired(true)
                        .certificate(Bytes.EMPTY)
                        .build())
                .build();
        final Path file = dir.resolve("known-publishers.json");
        Files.writeString(file, NetworkData.JSON.toJSON(data));

        final NetworkData loaded = BlockNodeApp.loadNetworkData(file);

        assertEquals(1, loaded.activeEndpoints().size());
        assertEquals("pub.example.com", loaded.activeEndpoints().get(0).remote().address());
    }

    @Test
    @DisplayName("A missing file yields an empty NetworkData")
    void missingFileReturnsEmpty(@TempDir final Path dir) {
        assertTrue(BlockNodeApp.loadNetworkData(dir.resolve("does-not-exist.json"))
                .activeEndpoints()
                .isEmpty());
    }

    @Test
    @DisplayName("A null path yields an empty NetworkData")
    void nullPathReturnsEmpty() {
        assertTrue(BlockNodeApp.loadNetworkData(null).activeEndpoints().isEmpty());
    }
}
