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
    /// Set to 300 MB: large enough to comfortably accommodate the biggest record files and blocks
    /// the tool handles, while still bounding allocation so a limit is encountered here rather than
    /// later in the Block Node. This guards against runaway allocation on corrupt input.
    public static final int MAX_MESSAGE_SIZE = 300 * 1024 * 1024;

    private ProtobufParsingConstants() {}
}
