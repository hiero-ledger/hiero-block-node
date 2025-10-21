package org.hiero.block.tools.commands.days.model;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.luben.zstd.ZstdInputStream;
import java.io.InputStream;
import java.util.List;
import org.hiero.block.tools.records.InMemoryFile;
import org.hiero.block.tools.utils.TarReader;
import org.junit.jupiter.api.Test;

/**
 * Test class for TarReader to verify reading of zstd-compressed tar streams.
 */
public class TarReaderTest {

    @Test
    public void readsZstdCompressedTarStream() throws Exception {
        try (InputStream res = TarReaderTest.class.getResourceAsStream("/2019-09-13.tar.zstd")) {
            assertNotNull(res, "test resource must exist: 2019-09-13.tar.zstd");
            try (ZstdInputStream zis = new ZstdInputStream(res);
                 var stream = TarReader.readTarContents(zis)) {
                List<InMemoryFile> files = stream.toList();
                // Sanity checks: we expect at least one file and each file should have a path and data array
                assertFalse(files.isEmpty(), "Expected at least one entry in the tar archive");
                boolean anyNonEmpty = files.stream().anyMatch(f -> f.data() != null && f.data().length > 0);
                assertTrue(anyNonEmpty, "Expected at least one entry with non-empty data");
            }
        }
    }
}
