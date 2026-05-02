// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.rsa;

import java.util.ArrayList;
import java.util.List;

/// Minimal parser for the Mirror Node `GET /api/v1/network/nodes` JSON response.
///
/// Only the fields needed to build a `NodeAddressBook` are extracted:
/// - `node_id` (long)
/// - `public_key` (hex-encoded DER RSA public key, may have a `0x` prefix)
/// - `links.next` (URL for the next page, `null` when the last page is reached)
///
/// This is a hand-rolled parser over raw JSON to avoid adding a JSON library dependency
/// to the module. It relies on the stable structure of the Mirror Node REST API response.
final class MirrorNodeNodesResponse {

    /// A single node entry extracted from the Mirror Node response.
    ///
    /// @param nodeId    the numeric node identifier
    /// @param publicKey the hex-encoded RSA public key (may include `0x` prefix, may be `null`)
    record NodeEntry(long nodeId, String publicKey) {}

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
    ///     { "node_id": 0, "public_key": "0x..." },
    ///     ...
    ///   ],
    ///   "links": { ... }
    /// }
    /// ```
    ///
    /// @param json the raw JSON string from the Mirror Node API
    /// @return parsed response containing node entries and the next-page link
    static MirrorNodeNodesResponse parse(final String json) {
        final List<NodeEntry> entries = new ArrayList<>();
        String nextLink = null;

        // Extract "nodes" array
        final int nodesStart = json.indexOf("\"nodes\"");
        if (nodesStart >= 0) {
            final int arrayOpen = json.indexOf('[', nodesStart);
            if (arrayOpen >= 0) {
                final int arrayClose = findMatchingBracket(json, arrayOpen, '[', ']');
                final String nodesArray = json.substring(arrayOpen + 1, arrayClose);

                // Iterate over each object {} in the array
                int objStart = 0;
                while ((objStart = nodesArray.indexOf('{', objStart)) >= 0) {
                    final int objEnd = findMatchingBracket(nodesArray, objStart, '{', '}');
                    final String obj = nodesArray.substring(objStart, objEnd + 1);

                    final long nodeId = extractLong(obj, "node_id");
                    final String publicKey = extractString(obj, "public_key");
                    entries.add(new NodeEntry(nodeId, publicKey));

                    objStart = objEnd + 1;
                }
            }
        }

        // Extract "links" -> "next"
        final int linksStart = json.indexOf("\"links\"");
        if (linksStart >= 0) {
            final int linksObjOpen = json.indexOf('{', linksStart);
            if (linksObjOpen >= 0) {
                final int linksObjClose = findMatchingBracket(json, linksObjOpen, '{', '}');
                final String linksObj = json.substring(linksObjOpen, linksObjClose + 1);
                final String next = extractString(linksObj, "next");
                if (next != null && !next.equals("null") && !next.isBlank()) {
                    nextLink = next;
                }
            }
        }

        return new MirrorNodeNodesResponse(entries, nextLink);
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /// Finds the index of the closing bracket/brace that matches the opener at `openPos`.
    ///
    /// @param s       the string to search in
    /// @param openPos the index of the opening bracket/brace
    /// @param open    the opening character (`{` or `[`)
    /// @param close   the closing character (`}` or `]`)
    /// @return the index of the matching closing character
    /// @throws IllegalArgumentException if the brackets are unbalanced
    private static int findMatchingBracket(final String s, final int openPos, final char open, final char close) {
        int depth = 0;
        for (int i = openPos; i < s.length(); i++) {
            final char c = s.charAt(i);
            if (c == open) depth++;
            else if (c == close) {
                depth--;
                if (depth == 0) return i;
            }
        }
        throw new IllegalArgumentException("Unbalanced brackets in Mirror Node response");
    }

    /// Extracts a `long` value for the given JSON key from the object string `obj`.
    ///
    /// @param obj the JSON object string
    /// @param key the key to look up
    /// @return the parsed long value, or -1 if the key is absent
    private static long extractLong(final String obj, final String key) {
        final int keyIdx = obj.indexOf("\"" + key + "\"");
        if (keyIdx < 0) return -1L;
        final int colon = obj.indexOf(':', keyIdx);
        int start = colon + 1;
        while (start < obj.length() && (obj.charAt(start) == ' ' || obj.charAt(start) == '\t')) start++;
        int end = start;
        while (end < obj.length() && (Character.isDigit(obj.charAt(end)) || obj.charAt(end) == '-')) end++;
        return Long.parseLong(obj.substring(start, end).trim());
    }

    /// Extracts a string value (without surrounding quotes) for the given JSON key from `obj`.
    /// Returns `null` if the key is absent or the value is JSON `null`.
    ///
    /// @param obj the JSON object string
    /// @param key the key to look up
    /// @return the extracted string value, or `null`
    private static String extractString(final String obj, final String key) {
        final int keyIdx = obj.indexOf("\"" + key + "\"");
        if (keyIdx < 0) return null;
        final int colon = obj.indexOf(':', keyIdx);
        int start = colon + 1;
        while (start < obj.length() && (obj.charAt(start) == ' ' || obj.charAt(start) == '\t')) start++;
        if (start >= obj.length()) return null;
        if (obj.charAt(start) == '"') {
            // quoted string
            final int end = obj.indexOf('"', start + 1);
            if (end < 0) return null;
            return obj.substring(start + 1, end);
        } else if (obj.startsWith("null", start)) {
            return null;
        }
        return null;
    }
}
