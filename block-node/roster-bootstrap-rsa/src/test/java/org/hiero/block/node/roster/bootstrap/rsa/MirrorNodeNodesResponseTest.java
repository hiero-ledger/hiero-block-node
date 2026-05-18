// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.rsa;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.pbj.runtime.Codec;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Unit tests for `MirrorNodeNodesResponse` JSON parsing.
class MirrorNodeNodesResponseTest {

    @Test
    @DisplayName("Parses single node entry with 0x-prefixed key correctly")
    void parseSingleNodeWithOxPrefix() throws ParseException {
        final String json = """
                {
                  "nodes": [
                    { "node_id": 0, "public_key": "0xdeadbeef" }
                  ],
                  "links": { "next": null }
                }
                """;
        final MirrorNodeNodesResponse response =
                MirrorNodeNodesResponse.JSON.parse(Bytes.wrap(json), false, Codec.DEFAULT_MAX_SIZE);
        assertEquals(1, response.nodes().size());
        assertEquals(0L, response.nodes().get(0).nodeId());
        assertEquals("0xdeadbeef", response.nodes().get(0).publicKey());
        assertNotNull(response.links());
        assertTrue(response.links().next().isEmpty());
    }

    @Test
    @DisplayName("Parses multiple nodes and extracts next-page link")
    void parseMultipleNodesWithNextLink() throws ParseException {
        final String json = """
                {
                  "nodes": [
                    { "node_id": 0, "public_key": "aabbcc" },
                    { "node_id": 1, "public_key": "ddeeff" }
                  ],
                  "links": { "next": "/api/v1/network/nodes?limit=100&order=asc&node.id=gt:1" }
                }
                """;
        final MirrorNodeNodesResponse response = MirrorNodeNodesResponse.JSON.parse(Bytes.wrap(json));
        assertEquals(2, response.nodes().size());
        assertEquals(0L, response.nodes().get(0).nodeId());
        assertEquals("aabbcc", response.nodes().get(0).publicKey());
        assertEquals(1L, response.nodes().get(1).nodeId());
        assertEquals("ddeeff", response.nodes().get(1).publicKey());
        assertEquals(
                "/api/v1/network/nodes?limit=100&order=asc&node.id=gt:1",
                response.links().next());
    }

    @Test
    @DisplayName("Returns null for nodes with null public_key")
    void parsesNullPublicKey() throws ParseException {
        final String json = """
                {
                  "nodes": [ { "node_id": 5, "public_key": null } ],
                  "links": { "next": null }
                }
                """;
        final MirrorNodeNodesResponse response = MirrorNodeNodesResponse.JSON.parse(Bytes.wrap(json));
        assertEquals(1, response.nodes().size());
        assertNull(response.nodes().get(0).publicKey());
    }

    @Test
    @DisplayName("Returns empty list and null nextLink for empty nodes array")
    void parsesEmptyNodesArray() throws ParseException {
        final String json = """
                {
                  "nodes": [],
                  "links": { "next": null }
                }
                """;
        final MirrorNodeNodesResponse response = MirrorNodeNodesResponse.JSON.parse(Bytes.wrap(json));
        assertEquals(List.of(), response.nodes());
        assertNotNull(response.links());
        assertTrue(response.links().next().isEmpty());
    }

    @Test
    @DisplayName("Missing nodes key yields empty list")
    void parsesAbsentNodesKey() throws ParseException {
        final String json = """
                {
                  "links": { "next": null }
                }
                """;
        final MirrorNodeNodesResponse response =
                MirrorNodeNodesResponse.JSON.parse(Bytes.wrap(json), false, Codec.DEFAULT_MAX_SIZE);
        assertEquals(List.of(), response.nodes());
        assertNotNull(response.links());
        assertTrue(response.links().next().isEmpty());
    }

    @Test
    @DisplayName("Missing links key yields null nextLink")
    void parsesAbsentLinksKey() throws ParseException {
        final String json = """
                {
                  "nodes": [ { "node_id": 0, "public_key": "aabb" } ]
                }
                """;
        final MirrorNodeNodesResponse response = MirrorNodeNodesResponse.JSON.parse(Bytes.wrap(json));
        assertEquals(1, response.nodes().size());
        assertNull(response.links());
    }

    @Test
    @DisplayName("Blank next link is treated as null (no next page)")
    void parsesBlankNextLink() throws ParseException {
        final String json = """
                {
                  "nodes": [],
                  "links": { "next": "   " }
                }
                """;
        final MirrorNodeNodesResponse response = MirrorNodeNodesResponse.JSON.parse(Bytes.wrap(json));
        assertNotNull(response.links());
        assertTrue(response.links().next().isEmpty());
    }

    @Test
    @DisplayName("Relative next link is returned as-is; callers must resolve against base URL")
    void relativeLinkReturnedAsIs() throws ParseException {
        final String json = """
                {
                  "nodes": [],
                  "links": { "next": "/api/v1/network/nodes?limit=100&node.id=gt:5" }
                }
                """;
        final MirrorNodeNodesResponse response = MirrorNodeNodesResponse.JSON.parse(Bytes.wrap(json));
        assertNotNull(response.links());
        assertEquals(
                "/api/v1/network/nodes?limit=100&node.id=gt:5", response.links().next());
    }

    @Test
    @DisplayName(
            "Active entry (timestamp.to=null) has timestamp with null to; historical (to non-null) has non-null to")
    void parsesTimestampFields() throws ParseException {
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
        final MirrorNodeNodesResponse response =
                MirrorNodeNodesResponse.JSON.parse(Bytes.wrap(json), true, Codec.DEFAULT_MAX_SIZE);
        assertEquals(2, response.nodes().size());

        final NodeEntry active = response.nodes().get(0);
        assertNotNull(active.timestamp());
        assertEquals("1000000000.000000000", active.timestamp().from());
        assertTrue(active.timestamp().to().isEmpty(), "Active entry must have null to");

        final NodeEntry historical = response.nodes().get(1);
        assertNotNull(historical.timestamp());
        assertEquals("1000000000.000000000", historical.timestamp().to());
    }

    @Test
    @DisplayName("Missing timestamp field yields null timestamp on NodeEntry")
    void parsesAbsentTimestamp() throws ParseException {
        final String json = """
                {
                  "nodes": [ { "node_id": 0, "public_key": "aabb" } ],
                  "links": { "next": null }
                }
                """;
        final MirrorNodeNodesResponse response = MirrorNodeNodesResponse.JSON.parse(Bytes.wrap(json));
        assertNull(response.nodes().get(0).timestamp(), "Absent timestamp field must be null");
    }
}
