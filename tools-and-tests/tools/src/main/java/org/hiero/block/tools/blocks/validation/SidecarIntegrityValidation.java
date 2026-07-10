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
     * {@code sidecarMetadatas}. Shared entry point so the wrap-time paths
     * ({@code ToWrappedBlocksCommand}, {@code LiveSequential}) can invoke the same check they'd
     * get on read-back through the validation suite / {@code validate-sidecars} command.
     *
     * <p>Passes silently on an empty {@code sidecarFiles} list. Throws {@link ValidationException}
     * on the first sidecar with no corresponding metadata entry; the message names the block
     * number, sidecar index, and computed hash.
     */
    public static void validateSidecars(
            final List<SidecarFile> sidecarFiles, final List<SidecarMetadata> sidecarMetadatas, final long blockNumber)
            throws ValidationException {
        if (sidecarFiles.isEmpty()) {
            return;
        }
        final MessageDigest digest = sha384Digest();
        for (int i = 0; i < sidecarFiles.size(); i++) {
            final SidecarFile sidecarFile = sidecarFiles.get(i);
            SidecarFile.PROTOBUF.toBytes(sidecarFile).writeTo(digest);
            final byte[] sidecarHash = digest.digest();
            if (!hashPresentIn(sidecarHash, sidecarMetadatas)) {
                throw new ValidationException("Block " + blockNumber + " - sidecar #" + i
                        + " SHA-384 " + hex(sidecarHash)
                        + " not found in signed sidecar metadata (" + sidecarMetadatas.size()
                        + " metadata entry/entries present)");
            }
        }
    }

    private static boolean hashPresentIn(final byte[] sidecarHash, final List<SidecarMetadata> sidecarMetadatas) {
        for (final SidecarMetadata meta : sidecarMetadatas) {
            if (!meta.hasHash()) {
                continue;
            }
            final byte[] expected = meta.hashOrThrow().hash().toByteArray();
            if (Arrays.equals(expected, sidecarHash)) {
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
