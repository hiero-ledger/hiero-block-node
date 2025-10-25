package org.hiero.block.tools.utils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assumptions;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

public class ZstCmdInputStreamTest {

    private static boolean zstdAvailable() {
        try {
            Process p = new ProcessBuilder("zstd", "--version").start();
            int exit = p.waitFor();
            return exit == 0;
        } catch (Exception e) {
            return false;
        }
    }

    @Test
    public void testDecompressResourceHasTarHeader() throws IOException, URISyntaxException {
        Assumptions.assumeTrue(zstdAvailable(), "zstd CLI not available, skipping test");

        URL res = getClass().getResource("/2019-09-13.tar.zstd");
        assertNotNull(res, "Test resource '/2019-09-13.tar.zstd' not found in test resources");
        Path resourcePath = Paths.get(res.toURI());
        try (InputStream in = new ZstCmdInputStream(resourcePath)) {
            byte[] header = new byte[512];
            int read = 0;
            while (read < header.length) {
                int r = in.read(header, read, header.length - read);
                if (r == -1) break;
                read += r;
            }
            assertTrue(read >= 263, "Expected to read at least 263 bytes for tar header but read " + read);
            // tar magic is at offset 257..262 and is "ustar\0"
            String magic = new String(header, 257, 5, StandardCharsets.US_ASCII);
            assertEquals("ustar", magic, "Tar 'ustar' magic expected at offset 257");
        }
    }
}
