// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.days.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
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

        final Path archive = buildFlatTarZstd(dayDir, tempDir.resolve("2026-08-03.tar.zstd"));

        try (var stream = TarZstdDayReaderUsingExec.streamTarZstd(archive)) {
            final List<UnparsedRecordBlock> blocks = stream.toList();
            assertEquals(recordCount, blocks.size(), "streamTarZstd should return every block in a flat archive");
        }

        // The list-based reader shares the same underlying grouping and never exhibited this bug;
        // asserting parity guards against the two diverging again in the future.
        final List<UnparsedRecordBlock> viaList = TarZstdDayReaderUsingExec.readTarZstd(archive);
        assertEquals(recordCount, viaList.size(), "readTarZstd should return every block in a flat archive");
    }

    /**
     * Regression test for a bug where a single record file still missing its signature file --
     * the normal, expected state of the chronologically newest record during a live/ongoing
     * download, since the CN uploads a record's {@code .rcd_sig} moments after its {@code .rcd}
     * -- aborted the grouping pass for the ENTIRE day before any block was streamed out, silently
     * discarding every other, fully-complete block in the same archive.
     */
    @Test
    public void streamTarZstd_trailingRecordMissingSignature_skipsOnlyThatRecord(@TempDir Path tempDir)
            throws Exception {
        assumeTrue(isAvailable("tar"), "Skipping test: tar not available");
        assumeTrue(isAvailable("zstd"), "Skipping test: zstd not available");

        final int recordCount = 10;
        final Path dayDir = tempDir.resolve("day");
        Files.createDirectories(dayDir);
        for (int i = 0; i < recordCount; i++) {
            final String ts = String.format("2026-08-03T10_00_%02d.000000000Z", i);
            Files.write(dayDir.resolve(ts + ".rcd"), new byte[] {0, 0, 0, 6, 0, 0, 0, 0});
            // Omit the signature file for only the chronologically last record -- simulating the
            // live "leading edge" where the .rcd has landed but its .rcd_sig hasn't uploaded yet.
            if (i < recordCount - 1) {
                Files.write(dayDir.resolve(ts + ".rcd_sig"), "fake-signature".getBytes());
            }
        }

        final Path archive = buildFlatTarZstd(dayDir, tempDir.resolve("2026-08-03.tar.zstd"));

        try (var stream = TarZstdDayReaderUsingExec.streamTarZstd(archive)) {
            final List<UnparsedRecordBlock> blocks = stream.toList();
            assertEquals(
                    recordCount - 1,
                    blocks.size(),
                    "streamTarZstd should skip only the trailing record missing its signature file");
        }
    }

    /**
     * A non-trailing record file missing its signature is _not_ wrapped with 0 signature files and a
     * warning rather than throwing — .rcd_sig files must not be absent when the record
     * source (e.g. Solo's MinIO) does not store them, WRBs are not TSS-signed blocks.
     * All _valid_ records (excluding the sig-less one) must be returned.
     */
    @Test
    public void streamTarZstd_middleRecordMissingSignature_excludesRecordWithNoSigs(@TempDir Path tempDir)
            throws IOException, InterruptedException {
        assumeTrue(isAvailable("tar"), "Skipping test: tar not available");
        assumeTrue(isAvailable("zstd"), "Skipping test: zstd not available");

        final int recordCount = 10;
        final int missingIndex = recordCount / 2;
        final Path dayDir = tempDir.resolve("day");
        Files.createDirectories(dayDir);
        for (int i = 0; i < recordCount; i++) {
            final String ts = String.format("2026-08-03T10_00_%02d.000000000Z", i);
            Files.write(dayDir.resolve(ts + ".rcd"), new byte[] {0, 0, 0, 6, 0, 0, 0, 0});
            if (i != missingIndex) {
                Files.write(dayDir.resolve(ts + ".rcd_sig"), "fake-signature".getBytes());
            }
        }

        assertThat(Files.list(dayDir).count()).isGreaterThanOrEqualTo(recordCount);
        final Path archive = buildFlatTarZstd(dayDir, tempDir.resolve("2026-08-03.tar.zstd"));

        try (var stream = TarZstdDayReaderUsingExec.streamTarZstd(archive)) {
            final List<UnparsedRecordBlock> blocks = stream.toList();
            assertEquals(
                    recordCount - 1,
                    blocks.size(),
                    "streamTarZstd should include all records except when the record has no sig file");
        }
    }

    /**
     * Mirrors detect-tss-enablement.sh / install-and-run-wrb-cli.sh's own archive creation:
     * {@code tar -cf - *.rcd *.rcd_sig | zstd -T0 > archive}, run from within the day directory so
     * entries are flat (no subdirectory prefix).
     */
    private static Path buildFlatTarZstd(Path dayDir, Path archive) throws IOException, InterruptedException {
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
        return archive;
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
