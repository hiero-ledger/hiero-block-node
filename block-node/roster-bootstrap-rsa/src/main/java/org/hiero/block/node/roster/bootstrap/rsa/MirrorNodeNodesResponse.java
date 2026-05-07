// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.rsa;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.ArrayList;
import java.util.List;

/// Minimal parser for the Mirror Node `GET /api/v1/network/nodes` JSON response.
///
/// Only the fields needed to build a `NodeAddressBook` are extracted:
/// - `node_id` (long)
/// - `public_key` (hex-encoded DER RSA public key, may have a `0x` prefix)
/// - `timestamp.from` / `timestamp.to` (validity window for this node configuration)
/// - `links.next` (URL for the next page, `null` when the last page is reached)
///
/// Uses Gson for JSON traversal rather than hand-rolled string parsing.
final class MirrorNodeNodesResponse {

    /// The validity window for a node configuration entry returned by the Mirror Node.
    ///
    /// The Mirror Node may return multiple historical records per physical node — one for each
    /// time the node's configuration changed. The **latest** (currently active) record has
    /// `to == null`. A non-null `to` means the record has been superseded by a newer one.
    ///
    /// Both values use Hedera's `<seconds>.<nanos>` timestamp format (e.g. `1734567890.123456789`).
    ///
    /// @param from the timestamp from which this configuration became active; non-null
    /// @param to   the timestamp at which this configuration was superseded, or `null` if still active
    record Timestamp(String from, String to) {}

    /// A single node entry extracted from the Mirror Node response.
    ///
    /// @param nodeId    the numeric node identifier
    /// @param publicKey the hex-encoded RSA public key (may include `0x` prefix, may be `null`)
    /// @param timestamp the validity window for this entry; `null` if the field was absent in the response
    record NodeEntry(long nodeId, String publicKey, Timestamp timestamp) {}

    private final List<NodeEntry> nodes;
    private final String nextLink;

    private MirrorNodeNodesResponse(final List<NodeEntry> nodes, final String nextLink) {
        this.nodes = List.copyOf(nodes);
        this.nextLink = nextLink;
    }

    /// Returns the list of node entries extracted from this response page.
    ///
    /// @return unmodifiable list of node entries
    List<NodeEntry> nodes() {
        return nodes;
    }

    /// Returns the URL for the next page of results, or `null` if this is the last page.
    ///
    /// @return next page URL, or `null`
    String nextLink() {
        return nextLink;
    }

    /// Parses a Mirror Node `GET /api/v1/network/nodes` JSON response body.
    ///
    /// The expected structure is:
    /// ```json
    /// {
    ///   "nodes": [
    ///     {
    ///       "node_id": 0,
    ///       "public_key": "0x...",
    ///       "timestamp": { "from": "1234567890.000000000", "to": null }
    ///     },
    ///     ...
    ///   ],
    ///   "links": { "next": "/api/v1/network/nodes?..." }
    /// }
    /// ```
    ///
    /// @param json the raw JSON string from the Mirror Node API
    /// @return parsed response containing node entries and the next-page link
    static MirrorNodeNodesResponse parse(final String json) {
        final List<NodeEntry> entries = new ArrayList<>();
        String nextLink = null;

        final JsonObject root = JsonParser.parseString(json).getAsJsonObject();

        // Extract "nodes" array
        final JsonArray nodes = root.getAsJsonArray("nodes");
        if (nodes != null) {
            for (final JsonElement element : nodes) {
                final JsonObject node = element.getAsJsonObject();
                final long nodeId = node.get("node_id").getAsLong();
                final JsonElement keyEl = node.get("public_key");
                final String publicKey = (keyEl == null || keyEl.isJsonNull()) ? null : keyEl.getAsString();
                final JsonElement tsEl = node.get("timestamp");
                Timestamp timestamp = null;
                if (tsEl != null && tsEl.isJsonObject()) {
                    final JsonObject ts = tsEl.getAsJsonObject();
                    final JsonElement fromEl = ts.get("from");
                    final JsonElement toEl = ts.get("to");
                    final String from = (fromEl == null || fromEl.isJsonNull()) ? null : fromEl.getAsString();
                    final String to = (toEl == null || toEl.isJsonNull()) ? null : toEl.getAsString();
                    timestamp = new Timestamp(from, to);
                }
                entries.add(new NodeEntry(nodeId, publicKey, timestamp));
            }
        }

        // Extract "links" -> "next"
        final JsonElement linksEl = root.get("links");
        if (linksEl != null && linksEl.isJsonObject()) {
            final JsonElement nextEl = linksEl.getAsJsonObject().get("next");
            if (nextEl != null && !nextEl.isJsonNull()) {
                final String next = nextEl.getAsString();
                if (!next.isBlank()) {
                    nextLink = next;
                }
            }
        }

        return new MirrorNodeNodesResponse(entries, nextLink);
    }
}
