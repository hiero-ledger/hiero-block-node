// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.rsa;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Unit tests for `MirrorNodeNodesResponse` JSON parsing.
class MirrorNodeNodesResponseTest {

    @Test
    @DisplayName("Parses single node entry with 0x-prefixed key correctly")
    void parseSingleNodeWithOxPrefix() {
        final String json = """
                {
                  "nodes": [
                    { "node_id": 0, "public_key": "0xdeadbeef" }
                  ],
                  "links": { "next": null }
                }
                """;
        final MirrorNodeNodesResponse response = MirrorNodeNodesResponse.parse(json);
        assertEquals(1, response.nodes().size());
        assertEquals(0L, response.nodes().get(0).nodeId());
        assertEquals("0xdeadbeef", response.nodes().get(0).publicKey());
        assertNull(response.nextLink());
    }

    @Test
    @DisplayName("Parses multiple nodes and extracts next-page link")
    void parseMultipleNodesWithNextLink() {
        final String json = """
                {
                  "nodes": [
                    { "node_id": 0, "public_key": "aabbcc" },
                    { "node_id": 1, "public_key": "ddeeff" }
                  ],
                  "links": { "next": "/api/v1/network/nodes?limit=100&order=asc&node.id=gt:1" }
                }
                """;
        final MirrorNodeNodesResponse response = MirrorNodeNodesResponse.parse(json);
        assertEquals(2, response.nodes().size());
        assertEquals(0L, response.nodes().get(0).nodeId());
        assertEquals("aabbcc", response.nodes().get(0).publicKey());
        assertEquals(1L, response.nodes().get(1).nodeId());
        assertEquals("ddeeff", response.nodes().get(1).publicKey());
        assertEquals("/api/v1/network/nodes?limit=100&order=asc&node.id=gt:1", response.nextLink());
    }

    @Test
    @DisplayName("Returns null for nodes with null public_key")
    void parsesNullPublicKey() {
        final String json = """
                {
                  "nodes": [ { "node_id": 5, "public_key": null } ],
                  "links": { "next": null }
                }
                """;
        final MirrorNodeNodesResponse response = MirrorNodeNodesResponse.parse(json);
        assertEquals(1, response.nodes().size());
        assertNull(response.nodes().get(0).publicKey());
    }

    @Test
    @DisplayName("Returns empty list and null nextLink for empty nodes array")
    void parsesEmptyNodesArray() {
        final String json = """
                {
                  "nodes": [],
                  "links": { "next": null }
                }
                """;
        final MirrorNodeNodesResponse response = MirrorNodeNodesResponse.parse(json);
        assertEquals(List.of(), response.nodes());
        assertNull(response.nextLink());
    }
}
