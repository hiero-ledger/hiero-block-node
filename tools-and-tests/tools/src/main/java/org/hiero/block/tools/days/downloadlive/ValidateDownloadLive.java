// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.downloadlive;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import org.hiero.block.tools.days.download.DownloadDayLiveImpl;
import org.hiero.block.tools.days.model.AddressBookRegistry;
import org.hiero.block.tools.days.subcommands.DownloadLive;
import org.hiero.block.tools.records.model.unparsed.InMemoryFile;
import org.hiero.block.tools.records.model.unparsed.UnparsedRecordBlockV6;

/**
 * Provides validation utilities for live-downloaded blockchain blocks.
 *
 * <p>This class handles the full validation pipeline for blocks downloaded in live mode,
 * including:
 * <ul>
 *   <li>Identifying and classifying downloaded files (primary records, signatures, sidecars)</li>
 *   <li>Performing cryptographic validation using the RecordFileBlockV6 pipeline</li>
 *   <li>Verifying signature consensus (at least 2/3 required)</li>
 *   <li>Validating hash chain continuity across blocks</li>
 *   <li>Quarantining problematic files for later inspection</li>
 * </ul>
 */
public class ValidateDownloadLive {

    /**
     * Finds the primary record file from the block download result.
     *
     * <p>The primary record file is identified by:
     * <ul>
     *   <li>Having a {@code .rcd} extension</li>
     *   <li>NOT containing {@code "_node_"} in its filename (which would indicate a node-specific copy)</li>
     * </ul>
     *
     * <p>In a typical block download, there is one primary record file and potentially
     * multiple node-specific record files (named like {@code ..._node_3.rcd}).
     *
     * @param result the block download result containing all downloaded files
     * @return the primary record file, or null if not found
     */
    private static InMemoryFile findPrimaryRecord(final DownloadDayLiveImpl.BlockDownloadResult result) {
        return result.files.stream()
                .filter(f -> {
                    final String name = f.path().getFileName().toString();
                    return name.endsWith(".rcd") && !name.contains("_node_");
                })
                .findFirst()
                .orElse(null);
    }

    /**
     * Finds all signature files from the block download result.
     *
     * <p>Signature files are used to cryptographically verify the authenticity of record files.
     * They can have different extensions depending on the format:
     * <ul>
     *   <li>{@code .rcs_sig} - Legacy signature format (e.g., {@code node_0.0.3.rcs_sig})</li>
     *   <li>{@code .rcd_sig} - Newer signature format</li>
     *   <li>{@code .rcd_sig.gz} - Compressed signature files</li>
     * </ul>
     *
     * <p>Typically, at least 2/3 of the signature files are required for successful
     * block validation (based on consensus threshold).
     *
     * @param result the block download result containing all downloaded files
     * @return a list of all signature files found; may be empty if none exist
     */
    private static List<InMemoryFile> findSignatures(final DownloadDayLiveImpl.BlockDownloadResult result) {
        return result.files.stream()
                .filter(f -> {
                    final String name = f.path().getFileName().toString();
                    // Accept legacy node_0.0.X.rcs_sig and any future rcd_sig variants
                    return name.endsWith(".rcs_sig") || name.endsWith(".rcd_sig") || name.endsWith(".rcd_sig.gz");
                })
                .toList();
    }

    /**
     * Finds sidecar record files associated with the given primary record file.
     *
     * <p>Sidecar files are additional record files that share the same timestamp prefix as the
     * primary record and are located in the same directory. They follow the naming pattern:
     * {@code <timestamp_prefix>_<index>.rcd}
     *
     * <p>For example, if the primary record is:
     * <pre>
     * 2025-12-01T00_00_04.319226458Z.rcd
     * </pre>
     * Then valid sidecar files would be:
     * <pre>
     * 2025-12-01T00_00_04.319226458Z_01.rcd
     * 2025-12-01T00_00_04.319226458Z_02.rcd
     * </pre>
     *
     * <p>Filtering criteria applied to identify sidecar files:
     * <ul>
     *   <li>Must have a {@code .rcd} extension</li>
     *   <li>Must be in the same directory as the primary record</li>
     *   <li>Must not be the primary record itself</li>
     *   <li>Must start with the same timestamp prefix as the primary (up to first underscore)</li>
     *   <li>Must have a numeric index suffix between the last underscore and {@code .rcd} extension</li>
     * </ul>
     *L
     * @param result the block download result containing all downloaded files for a block
     * @param primaryRecord the primary record file to find sidecars for; may be null
     * @return a list of sidecar files matching the primary record's timestamp, or an empty list
     *         if primaryRecord is null or no sidecars are found
     */
    private static List<InMemoryFile> findSidecars(
            final DownloadDayLiveImpl.BlockDownloadResult result, final InMemoryFile primaryRecord) {

        if (primaryRecord == null) {
            return List.of();
        }

        final Path primaryDir = primaryRecord.path().getParent();
        final String primaryName = primaryRecord.path().getFileName().toString();
        // Extract timestamp prefix up to the first underscore (e.g., 2025-12-01T00_00_04.319226458Z)
        final int underscore = primaryName.indexOf('_');
        final String tsPrefix = underscore > 0 ? primaryName.substring(0, underscore) : primaryName;

        return result.files.stream()
                .filter(f -> {
                    final Path p = f.path();
                    final String name = p.getFileName().toString();

                    // Filter 1: Must be a record file
                    if (!name.endsWith(".rcd")) {
                        return false;
                    }

                    // Filter 2: Must be in the same directory as the primary record
                    if (!Objects.equals(p.getParent(), primaryDir)) {
                        return false;
                    }

                    // Filter 3: Exclude the primary record itself
                    if (p.equals(primaryRecord.path())) {
                        return false;
                    }

                    // Filter 4: Must share the same timestamp prefix and have an underscore suffix
                    if (!name.startsWith(tsPrefix + "_")) {
                        return false;
                    }

                    // Filter 5: Validate structure - must have numeric index between last underscore and .rcd
                    // Expected format: <timestamp>_<numeric_index>.rcd (e.g., ...Z_01.rcd)
                    final int lastUnderscore = name.lastIndexOf('_');
                    final int dot = name.lastIndexOf('.');
                    if (lastUnderscore < 0 || dot <= lastUnderscore) {
                        return false;
                    }

                    // Extract and validate the index part (must be all digits)
                    final String indexPart = name.substring(lastUnderscore + 1, dot);
                    for (int i = 0; i < indexPart.length(); i++) {
                        if (!Character.isDigit(indexPart.charAt(i))) {
                            return false;
                        }
                    }
                    return true;
                })
                .toList();
    }

    /**
     * Logs all files included in a block download result for debugging purposes.
     *
     * @param result the block download result containing all files for the block
     */
    private static void logFilesForBlock(final DownloadDayLiveImpl.BlockDownloadResult result) {
        System.out.println("[download] fullBlockValidate all files for block " + result.blockNumber + ":");
        result.files.forEach(f -> System.out.println("      file=" + f.path()));
    }

    /**
     * Logs the inputs to block validation for debugging and troubleshooting.
     *
     * <p>This method outputs a summary of validation inputs including:
     * <ul>
     *   <li>Starting running hash from the previous block</li>
     *   <li>Record file timestamp</li>
     *   <li>Primary record file path</li>
     *   <li>Count and paths of signature files (up to 5 shown)</li>
     *   <li>Count and paths of sidecar files (up to 5 shown)</li>
     * </ul>
     *
     * <p>Long lists of signatures and sidecars are truncated to avoid log spam.
     *
     * @param startRunningHash the running hash from the previous block (may be null for first block)
     * @param recordFileTime the timestamp of the record file being validated
     * @param result the block download result
     * @param primaryRecord the primary record file (may be null if not found)
     * @param signatures the list of signature files
     * @param sidecars the list of sidecar files
     */
    private static void logValidationInput(
            final byte[] startRunningHash,
            final Instant recordFileTime,
            final DownloadDayLiveImpl.BlockDownloadResult result,
            final InMemoryFile primaryRecord,
            final List<InMemoryFile> signatures,
            final List<InMemoryFile> sidecars) {

        System.out.println("[download] fullBlockValidate input for block " + result.blockNumber + ":");
        System.out.println(
                "  startRunningHash=" + (startRunningHash == null ? "null" : DownloadLive.toHex(startRunningHash)));
        System.out.println("  recordFileTime=" + recordFileTime);
        System.out.println("  primaryRecord="
                + (primaryRecord == null ? "null" : primaryRecord.path().toString()));
        System.out.println("  signaturesCount=" + signatures.size());
        System.out.println("  sidecarsCount=" + sidecars.size());

        // Log up to a few entries for signatures and sidecars to avoid overly noisy logs
        final int sigLogLimit = 5;
        signatures.stream().limit(sigLogLimit).forEach(sig -> {
            final int index = signatures.indexOf(sig);
            System.out.println("    signature[" + index + "]=" + sig.path());
        });
        if (signatures.size() > sigLogLimit) {
            System.out.println("    ... " + (signatures.size() - sigLogLimit) + " more signature file(s) omitted");
        }

        final int scLogLimit = 5;
        sidecars.stream().limit(scLogLimit).forEach(sc -> {
            final int index = sidecars.indexOf(sc);
            System.out.println("    sidecar[" + index + "]=" + sc.path());
        });
        if (sidecars.size() > scLogLimit) {
            System.out.println("    ... " + (sidecars.size() - scLogLimit) + " more sidecar file(s) omitted");
        }
    }

    /**
     * Perform full block validation (record + signatures + sidecars) using the same
     * RecordFileBlockV6 pipeline as the offline Validate tool, but using the files
     * already downloaded for this block (no extra GCS fetches).
     *
     * @param startRunningHash the running hash from the previous block/file, may be null for the first block
     * @param recordFileTime   the record file time derived from mirror node timestamp
     * @param result           the block download result containing all in-memory files for this block
     * @param tmpFile          optional temp file path used only for quarantine bookkeeping (may be null)
     * @return true if full validation succeeds, false otherwise
     */
    public static boolean fullBlockValidate(
            final AddressBookRegistry addressBookRegistry,
            final byte[] startRunningHash,
            final Instant recordFileTime,
            final DownloadDayLiveImpl.BlockDownloadResult result,
            final Path tmpFile) {

        try {
            final InMemoryFile primaryRecord = findPrimaryRecord(result);
            final List<InMemoryFile> signatures = findSignatures(result);
            final List<InMemoryFile> sidecars = findSidecars(result, primaryRecord);

            System.out.println("[download] fullBlockValidate detected " + sidecars.size()
                    + " sidecar file(s) for block " + result.blockNumber);

            logFilesForBlock(result);
            logValidationInput(startRunningHash, recordFileTime, result, primaryRecord, signatures, sidecars);

            if (primaryRecord == null) {
                System.err.println("[download] No primary record found for block " + result.blockNumber
                        + "; skipping full validation.");
                return false;
            }

            final UnparsedRecordBlockV6 block = new UnparsedRecordBlockV6(
                    recordFileTime,
                    primaryRecord,
                    List.of(), // no "other" records in live mode
                    signatures,
                    sidecars,
                    List.of()); // no "other" sidecars in live mode

            final UnparsedRecordBlockV6.ValidationResult vr =
                    block.validate(startRunningHash, addressBookRegistry.getCurrentAddressBook());

            if (!vr.isValid()) {
                System.err.println("[download] Full block validation failed for block " + result.blockNumber + ": "
                        + vr.warningMessages());
                // tmpFile may be null; quarantine() is a no-op in that case.
                quarantine(
                        tmpFile,
                        primaryRecord.path().getFileName().toString(),
                        result.blockNumber,
                        "full block validation failure");
                return false;
            }

            final String addressBookChanges =
                    addressBookRegistry.updateAddressBook(block.recordFileTime(), vr.addressBookTransactions());
            if (addressBookChanges != null && !addressBookChanges.isBlank()) {
                System.out.println("[download] " + addressBookChanges);
            }

            return true;
        } catch (final Exception ex) {
            System.err.println("[download] Exception during full block validation for block " + result.blockNumber
                    + ": " + ex.getMessage());
            quarantine(tmpFile, "unknown", result.blockNumber, "exception during full block validation");
            return false;
        }
    }

    /**
     * Move a problematic temp file into a quarantine area under {@code outRoot} so that it can be
     * inspected later instead of being silently discarded.
     *
     * This is used when validation fails (e.g., block hash mismatch with mirror expectedHash).
     */
    private static void quarantine(Path tmpFile, String safeName, long blockNumber, String reason) {
        if (tmpFile == null) {
            return;
        }
        try {
            final Path quarantineDir = tmpFile.resolve("quarantine");
            Files.createDirectories(quarantineDir);
            final String targetName = blockNumber + "-" + safeName;
            final Path targetFile = quarantineDir.resolve(targetName);
            Files.move(tmpFile, targetFile, StandardCopyOption.REPLACE_EXISTING);
            System.err.println("[download] Quarantined file for block " + blockNumber + " (" + safeName + ") -> "
                    + targetFile + " reason=" + reason);
        } catch (IOException ioe) {
            System.err.println("[download] Failed to quarantine file for block " + blockNumber + " (" + safeName + "): "
                    + ioe.getMessage());
        }
    }
}
