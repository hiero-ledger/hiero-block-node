// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies that {@link SnapshotArchiver} produces a well-formed UStar tar containing
 * the source directory tree, and that the atomic-move + temp-file dance behaves
 * correctly across two successive archives.
 */
class SnapshotArchiverTest {

    @Test
    void archivesDirectoryTreeIntoTarWithExpectedEntries(@TempDir final Path tmp) throws IOException {
        final Path source = tmp.resolve("recent/42");
        Files.createDirectories(source.resolve("data/state"));
        Files.writeString(source.resolve("data/state/table_metadata.pbj"), "hello");
        Files.writeString(source.resolve("data/state/foo.ll"), "world");

        final Path historicRoot = tmp.resolve("historic");
        final Path tar = SnapshotArchiver.archive(source, historicRoot, 42L);

        assertThat(tar).exists();
        assertThat(tar.getFileName().toString()).isEqualTo("42.tar");
        assertThat(historicRoot.resolve("42.tar.tmp")).doesNotExist();

        final Map<String, byte[]> entries = readTar(Files.readAllBytes(tar));
        assertThat(entries.keySet())
                .as("tar contents")
                .containsExactlyInAnyOrder(
                        "42/",
                        "42/data/",
                        "42/data/state/",
                        "42/data/state/foo.ll",
                        "42/data/state/table_metadata.pbj");
        assertThat(new String(entries.get("42/data/state/table_metadata.pbj"), StandardCharsets.UTF_8))
                .isEqualTo("hello");
        assertThat(new String(entries.get("42/data/state/foo.ll"), StandardCharsets.UTF_8))
                .isEqualTo("world");
    }

    @Test
    void secondArchiveReplacesFirstAndLeavesNoTempFile(@TempDir final Path tmp) throws IOException {
        final Path source = tmp.resolve("recent/7");
        Files.createDirectories(source);
        Files.writeString(source.resolve("a"), "1");

        final Path historicRoot = tmp.resolve("historic");
        SnapshotArchiver.archive(source, historicRoot, 7L);
        final Map<String, byte[]> first = readTar(Files.readAllBytes(historicRoot.resolve("7.tar")));

        // Replace contents and archive again.
        Files.writeString(source.resolve("a"), "12345");
        SnapshotArchiver.archive(source, historicRoot, 7L);
        final Map<String, byte[]> second = readTar(Files.readAllBytes(historicRoot.resolve("7.tar")));

        assertThat(new String(first.get("7/a"), StandardCharsets.UTF_8)).isEqualTo("1");
        assertThat(new String(second.get("7/a"), StandardCharsets.UTF_8)).isEqualTo("12345");
        assertThat(historicRoot.resolve("7.tar.tmp")).doesNotExist();
    }

    // ── Tar reader ─────────────────────────────────────────────────────────

    /**
     * Bare-bones UStar reader for assertions. Returns an ordered map of entry name → content
     * bytes (empty for directory entries).
     */
    private static Map<String, byte[]> readTar(final byte[] bytes) throws IOException {
        final Map<String, byte[]> entries = new LinkedHashMap<>();
        try (InputStream in = new ByteArrayInputStream(bytes)) {
            while (true) {
                final byte[] header = in.readNBytes(512);
                if (header.length < 512) {
                    break;
                }
                // End-of-archive: all-zero block.
                boolean zero = true;
                for (final byte b : header) {
                    if (b != 0) {
                        zero = false;
                        break;
                    }
                }
                if (zero) {
                    break;
                }

                int nameEnd = 0;
                while (nameEnd < 100 && header[nameEnd] != 0) {
                    nameEnd++;
                }
                final String name = new String(header, 0, nameEnd, StandardCharsets.UTF_8);
                final long size = parseOctal(header, 124, 12);
                final byte typeFlag = header[156];

                if (typeFlag == '5' || name.endsWith("/")) {
                    entries.put(name, new byte[0]);
                } else {
                    final byte[] content = in.readNBytes((int) size);
                    entries.put(name, content);
                    final int pad = (int) ((512 - (size % 512)) % 512);
                    if (pad > 0) {
                        in.readNBytes(pad);
                    }
                }
            }
        }
        return entries;
    }

    private static long parseOctal(final byte[] header, final int offset, final int length) {
        long value = 0;
        for (int i = 0; i < length; i++) {
            final byte b = header[offset + i];
            if (b == 0 || b == ' ') {
                continue;
            }
            value = (value << 3) + (b - '0');
        }
        return value;
    }
}
