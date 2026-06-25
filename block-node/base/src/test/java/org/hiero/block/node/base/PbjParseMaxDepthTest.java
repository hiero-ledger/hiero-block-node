// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hedera.hapi.node.base.Key;
import com.hedera.pbj.runtime.Codec;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import org.junit.jupiter.api.Test;

/**
 * Verifies PBJ parse max-depth behavior as tracked in issue #3056.
 *
 * <p>PBJ 0.15.9 reduced {@code DEFAULT_MAX_DEPTH} from 512 to 128. All production
 * {@code .parse()} call sites must pass an explicit max depth of 256 so they are not
 * silently broken by future library changes.
 */
class PbjParseMaxDepthTest {

    /**
     * Documents the DEFAULT_MAX_DEPTH value introduced in PBJ 0.15.9.
     * If this assertion fails after a PBJ version bump, review every .parse() call site
     * and ensure it passes an explicit max depth of 256 (see issue #3056).
     */
    @Test
    void defaultMaxDepthIs128InPbj0_15_9() {
        assertThat(Codec.DEFAULT_MAX_DEPTH).isEqualTo(128);
    }

    /**
     * Demonstrates that a Key nested beyond DEFAULT_MAX_DEPTH levels fails with the
     * no-arg parse() overload that relies on the library default.
     *
     * <p>65 cycles × 2 depth units per cycle = 130 > DEFAULT_MAX_DEPTH (128).
     */
    @Test
    void deeplyNestedKeyFailsWithDefaultMaxDepth() {
        final Bytes deeplyNestedKey = buildDeeplyNestedKey(65);

        assertThatThrownBy(() -> Key.PROTOBUF.parse(deeplyNestedKey))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("depth");
    }

    /**
     * Demonstrates that the same structure that fails with the default depth succeeds
     * when parsed with an explicit max depth of 256.
     *
     * <p>65 cycles × 2 depth units per cycle = 130 < 256 (explicit max depth).
     */
    @Test
    void deeplyNestedKeySucceedsWithExplicitMaxDepth256() {
        final Bytes deeplyNestedKey = buildDeeplyNestedKey(65);

        assertThatNoException().isThrownBy(() -> Key.PROTOBUF.parse(deeplyNestedKey, false, 256));
    }

    /**
     * Constructs a {@link Bytes} buffer representing a {@link Key} nested {@code cycles}
     * times via the Key → KeyList → Key recursion.
     *
     * <p>Each cycle uses two depth units during parsing:
     * <ul>
     *   <li>Key parser descends into KeyList (field 6, tag 0x32): depth − 1</li>
     *   <li>KeyList parser descends into Key (field 1, tag 0x0A): depth − 1</li>
     * </ul>
     */
    private static Bytes buildDeeplyNestedKey(final int cycles) {
        byte[] keyBytes = new byte[0]; // innermost empty Key
        for (int i = 0; i < cycles; i++) {
            // field 1 (KeyList.keys): tag 0x0A, wire type LEN
            final byte[] keyListBytes = wrapInLenField(0x0A, keyBytes);
            // field 6 (Key.keyList): tag 0x32, wire type LEN
            keyBytes = wrapInLenField(0x32, keyListBytes);
        }
        return Bytes.wrap(keyBytes);
    }

    private static byte[] wrapInLenField(final int tag, final byte[] content) {
        final byte[] encodedLength = encodeVarint(content.length);
        final byte[] result = new byte[1 + encodedLength.length + content.length];
        result[0] = (byte) tag;
        System.arraycopy(encodedLength, 0, result, 1, encodedLength.length);
        System.arraycopy(content, 0, result, 1 + encodedLength.length, content.length);
        return result;
    }

    private static byte[] encodeVarint(final int value) {
        if (value < 128) {
            return new byte[] {(byte) value};
        } else if (value < 16_384) {
            return new byte[] {(byte) (0x80 | (value & 0x7F)), (byte) (value >>> 7)};
        } else {
            return new byte[] {
                (byte) (0x80 | (value & 0x7F)), (byte) (0x80 | ((value >>> 7) & 0x7F)), (byte) (value >>> 14)
            };
        }
    }
}
