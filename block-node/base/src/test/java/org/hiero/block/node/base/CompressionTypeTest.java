// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Unit tests for the {@link CompressionType} enum.
 */
@DisplayName("CompressionType Tests")
class CompressionTypeTest {
    /**
     * Functionality tests for the {@link CompressionType} enum.
     */
    @Nested
    @DisplayName("Functionality Tests")
    final class FunctionalityTests {
        /**
         * This test aims to verify the correct file extension is returned for
         * each compression type.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test extension() correctly returns the file extension based on the compression type")
        void testExtension(final CompressionType compressionType) {
            final String expected = compressionType.extension();
            switch (compressionType) {
                case ZSTD -> assertThat(".zstd").isEqualTo(expected);
                case NONE -> assertThat("").isEqualTo(expected);
            }
        }

        /**
         * This test aims to verify that a given input stream is correctly
         * wrapped based on the compression type.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test wrapStream(InputStream) correctly wraps the input stream based on the compression type")
        void testWrapStreamInputStream(final CompressionType compressionType) throws IOException {
            final ByteArrayInputStream original = new ByteArrayInputStream(new byte[0]);
            final InputStream actual = compressionType.wrapStream(original);
            switch (compressionType) {
                case ZSTD -> assertThat(actual)
                        .isNotNull()
                        .isExactlyInstanceOf(ZstdInputStream.class)
                        .isNotSameAs(original);
                case NONE -> assertThat(actual)
                        .isNotNull()
                        .isExactlyInstanceOf(original.getClass())
                        .isSameAs(original);
            }
        }

        /**
         * This test aims to verify that a given output stream is correctly
         * wrapped based on the compression type.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test wrapStream(OutputStream) correctly wraps the output stream based on the compression type")
        void testWrapStreamOutputStream(final CompressionType compressionType) throws IOException {
            final ByteArrayOutputStream original = new ByteArrayOutputStream();
            final OutputStream actual = compressionType.wrapStream(original);
            switch (compressionType) {
                case ZSTD -> assertThat(actual)
                        .isNotNull()
                        .isExactlyInstanceOf(ZstdOutputStream.class)
                        .isNotSameAs(original);
                case NONE -> assertThat(actual)
                        .isNotNull()
                        .isExactlyInstanceOf(ByteArrayOutputStream.class)
                        .isSameAs(original);
            }
        }

        /**
         * This test aims to verify that the compression works correctly for the
         * wrapped output stream.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test writeStream() correctly compresses the data written to the output stream")
        void testWriteStream(final CompressionType compressionType) throws IOException {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final OutputStream wrappedBaos = compressionType.wrapStream(baos);
            final byte[] expected = "test expected data".getBytes();
            wrappedBaos.write(expected);
            wrappedBaos.close(); // close always because otherwise some footers may not be written
            // decompress the data using the input compression type in order to be able to compare
            final byte[] actual = compressionType.decompress(baos.toByteArray());
            assertArrayEquals(expected, actual);
        }

        /**
         * This test aims to verify that the decompression works correctly for the
         * wrapped input stream.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test readStream() correctly decompresses the data read from the input stream")
        void testReadStream(final CompressionType compressionType) throws IOException {
            final byte[] expected = "test expected data".getBytes();
            // make sure to compress the data that will be read through the wrapped input stream
            final ByteArrayInputStream bais = new ByteArrayInputStream(compressionType.compress(expected));
            final InputStream wrappedBais = compressionType.wrapStream(bais);
            final byte[] actual = wrappedBais.readAllBytes();
            wrappedBais.close();
            assertArrayEquals(expected, actual);
        }
    }

    /**
     * This test aims to verify that the compression and decompression methods
     * work correctly for the compression type.
     */
    @ParameterizedTest
    @EnumSource(CompressionType.class)
    @DisplayName("Test compress() and decompress() methods")
    void testCompressAndDecompress(final CompressionType compressionType) {
        final byte[] expected = "test expected data".getBytes();
        final byte[] compressed = compressionType.compress(expected);
        final byte[] actual = compressionType.decompress(compressed);
        assertThat(actual).isEqualTo(expected).containsExactly(expected);
    }
}
