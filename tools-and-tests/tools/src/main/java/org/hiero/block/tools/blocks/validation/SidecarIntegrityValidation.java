// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import static org.hiero.block.tools.blocks.validation.BlockExtractionUtils.extractRecordFileBytes;
import static org.hiero.block.tools.blocks.validation.ProtobufParsingConstants.MAX_MESSAGE_SIZE;
import static org.hiero.block.tools.utils.Sha384.sha384Digest;

import com.hedera.hapi.block.stream.RecordFileItem;
import com.hedera.hapi.streams.RecordStreamFile;
import com.hedera.hapi.streams.SidecarFile;
import com.hedera.hapi.streams.SidecarMetadata;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.tools.records.model.parsed.ValidationException;

/**
 * Verifies that every {@code SidecarFile} embedded in a wrapped block matches a SHA-384 hash
 * listed in the record file's signed {@code sidecars[]} metadata.
 *
 * <p>The record file's RSA signature covers the sidecar hash list but not the sidecar bytes
 * themselves. Without this validation, a byte-vs-hash divergence at production time (or in
 * transit between the recordstream store and the wrapped block) would be silently baked into
 * the wrapped block and accepted downstream. See issue #3196 for background.
 *
 * <p>The check operates directly on the wrapped block: both the raw sidecar bytes
 * ({@code RecordFileItem.sidecarFileContents}) and the signed hash list
 * ({@code RecordFileItem.recordFileContents.sidecars[]}) are embedded at wrap time, so
 * historical blocks can be validated retroactively without re-wrapping.
 *
 * <p>Behaviour:
 * <ul>
 *   <li>Blocks with no sidecars pass trivially.</li>
 *   <li>Blocks with sidecars but no {@code recordFileContents} fail: no signed hash list to
 *       check against.</li>
 *   <li>Every sidecar must have a matching hash in the signed list; any sidecar without a match
 *       fails the block with a diagnostic that names the sidecar index and computed hash.</li>
 * </ul>
 */
public final class SidecarIntegrityValidation implements BlockValidation {

    private static final int MAX_DEPTH = 512;

    @Override
    public String name() {
        return "Sidecar Integrity";
    }

    @Override
    public String description() {
        return "Verifies each sidecar file's SHA-384 hash matches an entry in the record file's signed sidecar metadata";
    }

    @Override
    public boolean requiresGenesisStart() {
        return false;
    }

    @Override
    public void validate(final BlockUnparsed block, final long blockNumber) throws ValidationException {
        try {
            final Bytes recordFileBytes = extractRecordFileBytes(block);
            if (recordFileBytes == null || recordFileBytes.length() == 0) {
                // No RecordFile item in this block. Sidecars only ship alongside RecordFile items,
                // so nothing to check. Structural presence of RecordFile is the job of
                // RequiredItemsValidation, not this one.
                return;
            }

            final RecordFileItem recordFileItem = RecordFileItem.PROTOBUF.parse(
                    recordFileBytes.toReadableSequentialData(), false, false, MAX_DEPTH, MAX_MESSAGE_SIZE);

            final List<SidecarFile> sidecarFiles = recordFileItem.sidecarFileContents();
            if (sidecarFiles.isEmpty()) {
                return;
            }

            if (!recordFileItem.hasRecordFileContents()) {
                throw new ValidationException("Block " + blockNumber + " has " + sidecarFiles.size()
                        + " sidecar file(s) but no recordFileContents to check them against");
            }
            final RecordStreamFile recordStreamFile = recordFileItem.recordFileContentsOrThrow();
            validateSidecars(sidecarFiles, recordStreamFile.sidecars(), blockNumber);
        } catch (final ParseException e) {
            throw new ValidationException("Block " + blockNumber + " - sidecar integrity check failed: "
                    + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    /**
     * Verify every {@link SidecarFile} in {@code sidecarFiles} has a matching SHA-384 hash in
     * {@code sidecarMetadatas}, and every metadata hash has a matching sidecar file. Shared
     * entry point so the wrap-time paths ({@code ToWrappedBlocksCommand}, {@code LiveSequential})
     * can invoke the same check they'd get on read-back through the validation suite /
     * {@code validate-sidecars} command.
     *
     * <p>Passes silently on an empty {@code sidecarFiles} list. On any discrepancy, aggregates
     * every issue across all sidecars and metadata entries and throws a single
     * {@link ValidationException} whose message classifies each issue by failure mode
     * (TAMPERED_OR_EXTRA — sidecar bytes with no matching signed hash; MISSING — signed hash
     * with no matching sidecar bytes). This tells an investigator which of the three
     * distinct scenarios the bad block hit:
     *
     * <ul>
     *   <li>Byte-for-byte tampering (counts match, hashes don't line up)</li>
     *   <li>Missing sidecar (fewer sidecar files than signed hashes)</li>
     *   <li>Extra sidecar (more sidecar files than signed hashes)</li>
     * </ul>
     */
    public static void validateSidecars(
            final List<SidecarFile> sidecarFiles, final List<SidecarMetadata> sidecarMetadatas, final long blockNumber)
            throws ValidationException {
        if (sidecarFiles.isEmpty()) {
            return;
        }

        // Compute each sidecar's SHA-384 up front so we can do the two-way cross-check without
        // rehashing.
        final MessageDigest digest = sha384Digest();
        final byte[][] sidecarHashes = new byte[sidecarFiles.size()][];
        for (int i = 0; i < sidecarFiles.size(); i++) {
            SidecarFile.PROTOBUF.toBytes(sidecarFiles.get(i)).writeTo(digest);
            sidecarHashes[i] = digest.digest();
        }

        // Extract the set of expected hashes from metadata, skipping entries with no hash field.
        final List<byte[]> metadataHashes = new ArrayList<>(sidecarMetadatas.size());
        for (final SidecarMetadata meta : sidecarMetadatas) {
            if (meta.hasHash()) {
                metadataHashes.add(meta.hashOrThrow().hash().toByteArray());
            }
        }

        // Pass 1: which sidecars have no match in the signed list?
        //   (bytes-vs-hash divergence — TAMPERED — or count mismatch — EXTRA)
        final List<String> discrepancies = new ArrayList<>();
        for (int i = 0; i < sidecarHashes.length; i++) {
            if (!containsHash(metadataHashes, sidecarHashes[i])) {
                discrepancies.add(String.format(
                        "sidecar #%d SHA-384 %s -> no matching hash in signed metadata (TAMPERED or EXTRA)",
                        i, hex(sidecarHashes[i])));
            }
        }

        // Pass 2: which signed hashes have no matching sidecar bytes?
        //   (dropped-sidecar-at-wrap — MISSING)
        for (int j = 0; j < metadataHashes.size(); j++) {
            final byte[] expected = metadataHashes.get(j);
            boolean found = false;
            for (final byte[] sidecarHash : sidecarHashes) {
                if (Arrays.equals(sidecarHash, expected)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                discrepancies.add(String.format(
                        "signed hash #%d SHA-384 %s -> no matching sidecar file in block (MISSING)", j, hex(expected)));
            }
        }

        if (discrepancies.isEmpty()) {
            return;
        }

        final StringBuilder sb = new StringBuilder();
        sb.append("Block ").append(blockNumber).append(" sidecar integrity failed:");
        sb.append("\n  sidecars in block:    ").append(sidecarFiles.size());
        sb.append("\n  signed hash entries:  ").append(metadataHashes.size());
        sb.append("\n  discrepancies:");
        for (final String d : discrepancies) {
            sb.append("\n    - ").append(d);
        }
        throw new ValidationException(sb.toString());
    }

    private static boolean containsHash(final List<byte[]> hashes, final byte[] target) {
        for (final byte[] h : hashes) {
            if (Arrays.equals(h, target)) {
                return true;
            }
        }
        return false;
    }

    private static String hex(final byte[] bytes) {
        final StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (final byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
