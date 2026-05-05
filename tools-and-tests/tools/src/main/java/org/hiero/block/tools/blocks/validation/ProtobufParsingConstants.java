// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

/**
 * Shared protobuf parsing constants used across validation and hashing classes.
 */
public final class ProtobufParsingConstants {

    /** Maximum protobuf nesting depth for parsing. */
    public static final int MAX_DEPTH = 512;

    /** Maximum record file size for protobuf parsing (128 MB). */
    public static final int MAX_RECORD_FILE_SIZE = 128 * 1024 * 1024;

    /** Maximum parse size for protobuf messages (120 MB) to handle large blocks and StateChanges items. */
    public static final int MAX_PARSE_SIZE = 120 * 1024 * 1024;

    private ProtobufParsingConstants() {}
}
