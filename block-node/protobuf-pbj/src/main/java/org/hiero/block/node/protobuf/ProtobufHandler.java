// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.protobuf;

import com.hedera.pbj.runtime.Codec;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.ReadableSequentialData;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Centralized helper for parsing protobuf messages with a single, node-wide maximum message size.
 *
 * <p>The limit is sourced from {@code protobuf.maxMessageSizeBytes} ({@code ProtobufConfig}) and
 * applied here so every module — including those that cannot depend on the configuration module —
 * parses with the same operator-configurable bound. {@code BlockNodeApp} calls
 * {@link #setMaxMessageSizeBytes(int)} once at start-up; until then a sensible default applies so
 * tests and tools work without wiring.
 */
public final class ProtobufHandler {

    /**
     * Default maximum protobuf message size in bytes, equal to the {@code protobuf.maxMessageSizeBytes}
     * configuration default. Used until {@link #setMaxMessageSizeBytes(int)} is called at start-up.
     */
    public static final int DEFAULT_MAX_MESSAGE_SIZE_BYTES = 131_072_000;

    /** The node-wide maximum protobuf message size in bytes, set once at start-up from configuration. */
    private static volatile int maxMessageSizeBytes = DEFAULT_MAX_MESSAGE_SIZE_BYTES;

    private ProtobufHandler() {}

    /**
     * Set the node-wide maximum protobuf message size. Called once by {@code BlockNodeApp} at start-up
     * with the configured {@code protobuf.maxMessageSizeBytes} value.
     *
     * @param maxMessageSizeBytes the maximum protobuf message size in bytes
     */
    public static void setMaxMessageSizeBytes(final int maxMessageSizeBytes) {
        ProtobufHandler.maxMessageSizeBytes = maxMessageSizeBytes;
    }

    /**
     * The node-wide maximum protobuf message size in bytes used when parsing block data.
     *
     * @return the maximum protobuf message size in bytes
     */
    public static int maxMessageSizeBytes() {
        return maxMessageSizeBytes;
    }

    /**
     * Parse the given input with the supplied codec, applying the node-wide maximum message size.
     *
     * @param codec the PBJ codec
     * @param input the data to parse
     * @return the parsed value as {@link Bytes}
     * @throws ParseException if parsing fails
     */
    public static Bytes parse(@NonNull Codec codec, @NonNull ReadableSequentialData input) throws ParseException {
        return parse(codec, input, maxMessageSizeBytes);
    }

    /**
     * Parse the given input with the supplied codec and an explicit maximum message size.
     *
     * @param codec the PBJ codec
     * @param input the data to parse
     * @param maxMessageSizeBytes the maximum protobuf message size in bytes
     * @return the parsed value as {@link Bytes}
     * @throws ParseException if parsing fails
     */
    public static Bytes parse(@NonNull Codec codec, @NonNull ReadableSequentialData input, int maxMessageSizeBytes)
            throws ParseException {
        return (Bytes) codec.parse(input, false, true, maxMessageSizeBytes / 8, maxMessageSizeBytes);
    }
}
