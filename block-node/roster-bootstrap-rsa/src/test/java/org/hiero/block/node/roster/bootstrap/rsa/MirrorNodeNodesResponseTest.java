// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.rsa;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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

    @Test
    @DisplayName("Missing nodes key yields empty list")
    void parsesAbsentNodesKey() {
        final String json = """
                {
                  "links": { "next": null }
                }
                """;
        final MirrorNodeNodesResponse response = MirrorNodeNodesResponse.parse(json);
        assertEquals(List.of(), response.nodes());
        assertNull(response.nextLink());
    }

    @Test
    @DisplayName("Missing links key yields null nextLink")
    void parsesAbsentLinksKey() {
        final String json = """
                {
                  "nodes": [ { "node_id": 0, "public_key": "aabb" } ]
                }
                """;
        final MirrorNodeNodesResponse response = MirrorNodeNodesResponse.parse(json);
        assertEquals(1, response.nodes().size());
        assertNull(response.nextLink());
    }

    @Test
    @DisplayName("Blank next link is treated as null (no next page)")
    void parsesBlankNextLink() {
        final String json = """
                {
                  "nodes": [],
                  "links": { "next": "   " }
                }
                """;
        final MirrorNodeNodesResponse response = MirrorNodeNodesResponse.parse(json);
        assertNull(response.nextLink(), "Whitespace-only next link must be normalised to null");
    }

    @Test
    @DisplayName("Relative next link is returned as-is; callers must resolve against base URL")
    void relativeLinkReturnedAsIs() {
        final String json = """
                {
                  "nodes": [],
                  "links": { "next": "/api/v1/network/nodes?limit=100&node.id=gt:5" }
                }
                """;
        final MirrorNodeNodesResponse response = MirrorNodeNodesResponse.parse(json);
        assertEquals("/api/v1/network/nodes?limit=100&node.id=gt:5", response.nextLink());
    }

    @Test
    @DisplayName(
            "Active entry (timestamp.to=null) has timestamp with null to; historical (to non-null) has non-null to")
    void parsesTimestampFields() {
        final String json = """
                {
                  "nodes": [
                    {
                      "node_id": 0,
                      "public_key": "aabb",
                      "timestamp": { "from": "1000000000.000000000", "to": null }
                    },
                    {
                      "node_id": 1,
                      "public_key": "ccdd",
                      "timestamp": { "from": "900000000.000000000", "to": "1000000000.000000000" }
                    }
                  ],
                  "links": { "next": null }
                }
                """;
        final MirrorNodeNodesResponse response = MirrorNodeNodesResponse.parse(json);
        assertEquals(2, response.nodes().size());

        final MirrorNodeNodesResponse.NodeEntry active = response.nodes().get(0);
        assertNotNull(active.timestamp());
        assertEquals("1000000000.000000000", active.timestamp().from());
        assertNull(active.timestamp().to(), "Active entry must have null to");

        final MirrorNodeNodesResponse.NodeEntry historical = response.nodes().get(1);
        assertNotNull(historical.timestamp());
        assertEquals("1000000000.000000000", historical.timestamp().to());
    }

    @Test
    @DisplayName("Missing timestamp field yields null timestamp on NodeEntry")
    void parsesAbsentTimestamp() {
        final String json = """
                {
                  "nodes": [ { "node_id": 0, "public_key": "aabb" } ],
                  "links": { "next": null }
                }
                """;
        final MirrorNodeNodesResponse response = MirrorNodeNodesResponse.parse(json);
        assertNull(response.nodes().get(0).timestamp(), "Absent timestamp field must be null");
    }
}
