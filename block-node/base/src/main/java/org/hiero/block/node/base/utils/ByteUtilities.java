// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base.utils;

/// Utility class for byte array conversion operations.
public final class ByteUtilities {

    /// Converts an integer to a 4-byte array in little-endian format.
    ///
    /// In little-endian format, the least significant byte is placed first.
    /// For example, the integer `0x01020304` will be converted to the byte array
    /// `[0x04, 0x03, 0x02, 0x01]`.
    ///
    /// @param input the integer value to convert
    /// @return a 4-byte array representing the input in little-endian format
    public static byte[] intToByteArrayLittleEndian(int input) {
        return new byte[] {(byte) input, (byte) (input >>> 8), (byte) (input >>> 16), (byte) (input >>> 24)};
    }

    /// Converts an integer to a 4-byte array in big-endian format.
    ///
    /// In big-endian format, the most significant byte is placed first.
    /// For example, the integer `0x01020304` will be converted to the byte array
    /// `[0x01, 0x02, 0x03, 0x04]`.
    ///
    /// @param input the integer value to convert
    /// @return a 4-byte array representing the input in big-endian format
    public static byte[] intToByteArrayBigEndian(int input) {
        return new byte[] {(byte) (input >>> 24), (byte) (input >>> 16), (byte) (input >>> 8), (byte) input};
    }

    private ByteUtilities() {}
}
