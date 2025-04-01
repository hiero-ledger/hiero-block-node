// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base;

import static org.junit.jupiter.api.Assertions.*;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the CompressionType enum.
 */
public class CompressionTypeTest {

    @Test
    void testExtension() {
        assertEquals(".zstd", CompressionType.ZSTD.extension());
        assertEquals("", CompressionType.NONE.extension());
    }

    @Test
    void testWrapStreamInputStream() throws IOException {
        byte[] data = "test data".getBytes();
        InputStream inputStream = new ByteArrayInputStream(data);

        InputStream zstdStream = CompressionType.ZSTD.wrapStream(inputStream);
        assertNotNull(zstdStream);
        assertInstanceOf(ZstdInputStream.class, zstdStream);

        InputStream noneStream = CompressionType.NONE.wrapStream(inputStream);
        assertNotNull(noneStream);
        assertFalse(noneStream instanceof com.github.luben.zstd.ZstdInputStream);
    }

    @Test
    void testWrapStreamOutputStream() throws IOException {
        OutputStream outputStream = new ByteArrayOutputStream();

        OutputStream zstdStream = CompressionType.ZSTD.wrapStream(outputStream);
        assertNotNull(zstdStream);
        assertInstanceOf(ZstdOutputStream.class, zstdStream);

        OutputStream noneStream = CompressionType.NONE.wrapStream(outputStream);
        assertNotNull(noneStream);
        assertFalse(noneStream instanceof com.github.luben.zstd.ZstdOutputStream);
    }
}
