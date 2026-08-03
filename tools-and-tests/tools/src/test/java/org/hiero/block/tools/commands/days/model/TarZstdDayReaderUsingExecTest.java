// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.days.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.hiero.block.tools.days.model.TarZstdDayReaderUsingExec;
import org.hiero.block.tools.records.model.unparsed.UnparsedRecordBlock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression test for a bug where {@link TarZstdDayReaderUsingExec#streamTarZstd(Path)} silently
 * dropped all but the first block when reading a "flat" day archive (record/signature files stored
 * directly at the tar root, with no per-timestamp subdirectory) — the format produced by the
 * solo-e2e-test WRB distribution scripts, as opposed to the per-timestamp-directory layout used by
 * mirror-node-style day archives (see {@code 2019-09-13.tar.zstd}).
 */
public class TarZstdDayReaderUsingExecTest {

    @Test
    public void streamTarZstd_flatArchiveWithMultipleTimestamps_returnsAllBlocks(@TempDir Path tempDir)
            throws Exception {
        assumeTrue(isAvailable("tar"), "Skipping test: tar not available");
        assumeTrue(isAvailable("zstd"), "Skipping test: zstd not available");

        final int recordCount = 10;
        final Path dayDir = tempDir.resolve("day");
        Files.createDirectories(dayDir);
        for (int i = 0; i < recordCount; i++) {
            final String ts = String.format("2026-08-03T10_00_%02d.000000000Z", i);
            // Minimal valid V6 record file: 4-byte big-endian version (6) + 4-byte HAPI version.
            Files.write(dayDir.resolve(ts + ".rcd"), new byte[] {0, 0, 0, 6, 0, 0, 0, 0});
            Files.write(dayDir.resolve(ts + ".rcd_sig"), "fake-signature".getBytes());
        }

        final Path archive = tempDir.resolve("2026-08-03.tar.zstd");
        // Mirrors detect-tss-enablement.sh / install-and-run-wrb-cli.sh's own archive creation:
        // `tar -cf - *.rcd *.rcd_sig | zstd -T0 > archive`, run from within the day directory so
        // entries are flat (no subdirectory prefix).
        final ProcessBuilder pb = new ProcessBuilder(
                "sh", "-c", "tar -cf - *.rcd *.rcd_sig | zstd -T0 > '" + archive.toAbsolutePath() + "'");
        pb.directory(dayDir.toFile());
        pb.redirectErrorStream(true);
        // Prevents macOS tar from adding AppleDouble ("._filename") resource-fork sidecar entries
        // on APFS, which aren't real record files and would otherwise fail to parse; a no-op on
        // Linux CI runners.
        pb.environment().put("COPYFILE_DISABLE", "1");
        final Process proc = pb.start();
        final int exitCode = proc.waitFor();
        assertEquals(0, exitCode, "tar/zstd archive creation should succeed");

        try (var stream = TarZstdDayReaderUsingExec.streamTarZstd(archive)) {
            final List<UnparsedRecordBlock> blocks = stream.toList();
            assertEquals(recordCount, blocks.size(), "streamTarZstd should return every block in a flat archive");
        }

        // The list-based reader shares the same underlying grouping and never exhibited this bug;
        // asserting parity guards against the two diverging again in the future.
        final List<UnparsedRecordBlock> viaList = TarZstdDayReaderUsingExec.readTarZstd(archive);
        assertEquals(recordCount, viaList.size(), "readTarZstd should return every block in a flat archive");
    }

    private static boolean isAvailable(String cmd) {
        try {
            final Process p = new ProcessBuilder("which", cmd).start();
            return p.waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }
}
