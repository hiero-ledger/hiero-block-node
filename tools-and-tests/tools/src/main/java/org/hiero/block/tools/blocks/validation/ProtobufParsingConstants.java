// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

/**
 * Shared protobuf parsing constants used across validation and hashing classes.
 */
public final class ProtobufParsingConstants {

    /** Maximum protobuf nesting depth for parsing. */
    public static final int MAX_DEPTH = 512;

    /// Maximum size for parsing a whole record file, block, or other large message.
    ///
    /// Set to [Integer#MAX_VALUE] (effectively unbounded): these parses consume trusted data the
    /// tool itself produced or fetched, and message sizes are determined by network activity (e.g.
    /// oversized blocks), not by a bound we choose. The cap only guards against runaway allocation
    /// on corrupt input, which is not a concern here.
    public static final int MAX_MESSAGE_SIZE = Integer.MAX_VALUE;

    private ProtobufParsingConstants() {}
}
