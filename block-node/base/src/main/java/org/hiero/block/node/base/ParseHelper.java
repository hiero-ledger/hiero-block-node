// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base;

import com.hedera.pbj.runtime.Codec;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.ReadableSequentialData;
import com.hedera.pbj.runtime.io.buffer.Bytes;

/**
 * Convenience helpers for PBJ codec parsing.
 *
 * <p>PBJ 0.15.9 reduced {@code Codec.DEFAULT_MAX_DEPTH} from 512 to 128, which is insufficient
 * for block stream data. All parse call sites must use these helpers so the depth and unknown-field
 * settings are applied consistently and cannot silently regress (see issue #3056).
 */
public final class ParseHelper {

    private static final int MAX_PARSE_DEPTH = 256;

    private ParseHelper() {}

    /**
     * Parse protobuf/JSON bytes using standard settings: strict=false, unknownFields=true,
     * depth=256, maxSize={@link Codec#DEFAULT_MAX_SIZE}.
     */
    public static <T> T standardParse(final Codec<T> codec, final Bytes input) throws ParseException {
        return codec.parse(input.toReadableSequentialData(), false, true, MAX_PARSE_DEPTH, Codec.DEFAULT_MAX_SIZE);
    }

    /**
     * Parse protobuf/JSON bytes with a custom size limit. Use when the input is known to exceed
     * {@link Codec#DEFAULT_MAX_SIZE} (e.g. full block payloads).
     */
    public static <T> T standardParse(final Codec<T> codec, final Bytes input, final int maxSize)
            throws ParseException {
        return codec.parse(input.toReadableSequentialData(), false, true, MAX_PARSE_DEPTH, maxSize);
    }

    /**
     * Parse from a {@link ReadableSequentialData} using standard settings: strict=false,
     * unknownFields=true, depth=256, maxSize={@link Codec#DEFAULT_MAX_SIZE}.
     */
    public static <T> T standardParse(final Codec<T> codec, final ReadableSequentialData input) throws ParseException {
        return codec.parse(input, false, true, MAX_PARSE_DEPTH, Codec.DEFAULT_MAX_SIZE);
    }

    /**
     * Parse from a {@link ReadableSequentialData} with a custom size limit. Use when the input
     * is known to exceed {@link Codec#DEFAULT_MAX_SIZE} (e.g. full block payloads).
     */
    public static <T> T standardParse(final Codec<T> codec, final ReadableSequentialData input, final int maxSize)
            throws ParseException {
        return codec.parse(input, false, true, MAX_PARSE_DEPTH, maxSize);
    }
}
