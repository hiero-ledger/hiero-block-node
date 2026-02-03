// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.common.utils;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/// Tests for [ByteUtilities] functionality.
class ByteUtilitiesTest {

    /// This test verifies that [ByteUtilities#intToByteArrayLittleEndian(int)]
    /// correctly converts an integer to a little-endian byte array.
    @Test
    void testIntToByteArrayLittleEndianZero() {
        final byte[] result = ByteUtilities.intToByteArrayLittleEndian(0);
        assertThat(result).containsExactly(0, 0, 0, 0);
    }

    /// This test verifies that [ByteUtilities#intToByteArrayLittleEndian(int)]
    /// correctly converts a positive integer to a little-endian byte array.
    @Test
    void testIntToByteArrayLittleEndianPositive() {
        // 0x01020304 = 16909060
        final byte[] result = ByteUtilities.intToByteArrayLittleEndian(0x01020304);
        assertThat(result).containsExactly(0x04, 0x03, 0x02, 0x01);
    }

    /// This test verifies that [ByteUtilities#intToByteArrayLittleEndian(int)]
    /// correctly converts a negative integer to a little-endian byte array.
    @Test
    void testIntToByteArrayLittleEndianNegative() {
        final byte[] result = ByteUtilities.intToByteArrayLittleEndian(-1);
        assertThat(result).containsExactly((byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF);
    }

    /// This test verifies that [ByteUtilities#intToByteArrayLittleEndian(int)]
    /// correctly converts `Integer.MAX_VALUE` to a little-endian byte array.
    @Test
    void testIntToByteArrayLittleEndianMaxValue() {
        final byte[] result = ByteUtilities.intToByteArrayLittleEndian(Integer.MAX_VALUE);
        assertThat(result).containsExactly((byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x7F);
    }

    /// This test verifies that [ByteUtilities#intToByteArrayLittleEndian(int)]
    /// correctly converts `Integer.MIN_VALUE` to a little-endian byte array.
    @Test
    void testIntToByteArrayLittleEndianMinValue() {
        final byte[] result = ByteUtilities.intToByteArrayLittleEndian(Integer.MIN_VALUE);
        assertThat(result).containsExactly((byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x80);
    }

    /// This test verifies that [ByteUtilities#intToByteArrayLittleEndian(int)]
    /// produces arrays of the correct length for various input values.
    ///
    /// @param value parameterized, the integer value to test
    @ParameterizedTest
    @ValueSource(ints = {0, 1, -1, 100, -100, 12345, -12345, Integer.MAX_VALUE, Integer.MIN_VALUE})
    void testIntToByteArrayLittleEndianLength(final int value) {
        final byte[] result = ByteUtilities.intToByteArrayLittleEndian(value);
        assertThat(result).hasSize(4);
    }

    /// This test verifies that [ByteUtilities#intToByteArrayBigEndian(int)]
    /// correctly converts an integer to a big-endian byte array.
    @Test
    void testIntToByteArrayBigEndianZero() {
        final byte[] result = ByteUtilities.intToByteArrayBigEndian(0);
        assertThat(result).containsExactly(0, 0, 0, 0);
    }

    /// This test verifies that [ByteUtilities#intToByteArrayBigEndian(int)]
    /// correctly converts a positive integer to a big-endian byte array.
    @Test
    void testIntToByteArrayBigEndianPositive() {
        // 0x01020304 = 16909060
        final byte[] result = ByteUtilities.intToByteArrayBigEndian(0x01020304);
        assertThat(result).containsExactly(0x01, 0x02, 0x03, 0x04);
    }

    /// This test verifies that [ByteUtilities#intToByteArrayBigEndian(int)]
    /// correctly converts a negative integer to a big-endian byte array.
    @Test
    void testIntToByteArrayBigEndianNegative() {
        final byte[] result = ByteUtilities.intToByteArrayBigEndian(-1);
        assertThat(result).containsExactly((byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF);
    }

    /// This test verifies that [ByteUtilities#intToByteArrayBigEndian(int)]
    /// correctly converts `Integer.MAX_VALUE` to a big-endian byte array.
    @Test
    void testIntToByteArrayBigEndianMaxValue() {
        final byte[] result = ByteUtilities.intToByteArrayBigEndian(Integer.MAX_VALUE);
        assertThat(result).containsExactly((byte) 0x7F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF);
    }

    /// This test verifies that [ByteUtilities#intToByteArrayBigEndian(int)]
    /// correctly converts `Integer.MIN_VALUE` to a big-endian byte array.
    @Test
    void testIntToByteArrayBigEndianMinValue() {
        final byte[] result = ByteUtilities.intToByteArrayBigEndian(Integer.MIN_VALUE);
        assertThat(result).containsExactly((byte) 0x80, (byte) 0x00, (byte) 0x00, (byte) 0x00);
    }

    /// This test verifies that [ByteUtilities#intToByteArrayBigEndian(int)]
    /// produces arrays of the correct length for various input values.
    ///
    /// @param value parameterized, the integer value to test
    @ParameterizedTest
    @ValueSource(ints = {0, 1, -1, 100, -100, 12345, -12345, Integer.MAX_VALUE, Integer.MIN_VALUE})
    void testIntToByteArrayBigEndianLength(final int value) {
        final byte[] result = ByteUtilities.intToByteArrayBigEndian(value);
        assertThat(result).hasSize(4);
    }

    /// This test verifies that the little-endian and big-endian conversions
    /// produce reversed byte arrays for the same input value.
    ///
    /// @param value parameterized, the integer value to test
    @ParameterizedTest
    @ValueSource(ints = {0, 1, -1, 100, -100, 12345, -12345, 0x01020304, Integer.MAX_VALUE, Integer.MIN_VALUE})
    void testEndianConversionsAreReversed(final int value) {
        final byte[] littleEndian = ByteUtilities.intToByteArrayLittleEndian(value);
        final byte[] bigEndian = ByteUtilities.intToByteArrayBigEndian(value);

        assertThat(littleEndian[0]).isEqualTo(bigEndian[3]);
        assertThat(littleEndian[1]).isEqualTo(bigEndian[2]);
        assertThat(littleEndian[2]).isEqualTo(bigEndian[1]);
        assertThat(littleEndian[3]).isEqualTo(bigEndian[0]);
    }

    /// This test verifies byte order consistency for a simple single-byte value.
    @Test
    void testSingleByteValue() {
        final byte[] littleEndian = ByteUtilities.intToByteArrayLittleEndian(255);
        final byte[] bigEndian = ByteUtilities.intToByteArrayBigEndian(255);

        assertThat(littleEndian).containsExactly((byte) 0xFF, 0x00, 0x00, 0x00);
        assertThat(bigEndian).containsExactly(0x00, 0x00, 0x00, (byte) 0xFF);
    }

    /// This test verifies byte order consistency for a two-byte value.
    @Test
    void testTwoByteValue() {
        final byte[] littleEndian = ByteUtilities.intToByteArrayLittleEndian(0x1234);
        final byte[] bigEndian = ByteUtilities.intToByteArrayBigEndian(0x1234);

        assertThat(littleEndian).containsExactly((byte) 0x34, (byte) 0x12, 0x00, 0x00);
        assertThat(bigEndian).containsExactly(0x00, 0x00, (byte) 0x12, (byte) 0x34);
    }
}
