// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records.model.parsed;

import static org.hiero.block.tools.utils.Sha384.sha384Digest;

import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.streams.SidecarFile;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import org.hiero.block.tools.records.model.unparsed.UnparsedRecordBlock;

/**
 * In-memory representation of a parsed Hedera record block, including the primary record file,
 * associated signature files, and sidecar files. The parsed record block only contains the good data and none of the
 * other files like UnparsedRecordBlock.
 *
 * @param recordFile the parsed primary record file
 * @param signatureFiles the list of parsed signature files
 * @param sidecarFiles the list of sidecar files
 */
public record ParsedRecordBlock(
        ParsedRecordFile recordFile, List<ParsedSignatureFile> signatureFiles, List<SidecarFile> sidecarFiles) {

    /**
     * Get the consensus time of the block.
     *
     * @return the consensus time of the block
     */
    public Instant blockTime() {
        return recordFile.blockTime();
    }

    /**
     * Parse an unparsed record block into a parsed record block.
     *
     * @param recordFileBlock the unparsed record block
     * @return the parsed record block
     */
    public static ParsedRecordBlock parse(UnparsedRecordBlock recordFileBlock) {
        return new ParsedRecordBlock(
                ParsedRecordFile.parse(recordFileBlock.primaryRecordFile()),
                recordFileBlock.signatureFiles().stream()
                        .map(sigFile -> {
                            try {
                                return new ParsedSignatureFile(sigFile);
                            } catch (RuntimeException e) {
                                System.err.println("Warning: Skipping corrupted signature file: " + sigFile.path()
                                        + " (" + sigFile.data().length + " bytes) - " + e.getMessage());
                                return null;
                            }
                        })
                        .filter(sig -> sig != null)
                        .toList(),
                recordFileBlock.primarySidecarFiles().stream()
                        .map(ps -> {
                            try {
                                return SidecarFile.PROTOBUF.parse(Bytes.wrap(ps.data()));
                            } catch (ParseException e) {
                                throw new RuntimeException(e);
                            }
                        })
                        .toList());
    }

    /**
     * Write the parsed record block to the specified directory. This writes the record file, signature files, and
     * sidecar files in their original format versions.
     *
     * @param directory the directory to write to
     * @param gzipped whether to gzip the record file written
     * @throws IOException if an I/O error occurs
     */
    public void write(Path directory, boolean gzipped) throws IOException {
        // Write record file
        recordFile.write(directory, gzipped);
        // Write signature files
        for (ParsedSignatureFile signatureFile : signatureFiles) {
            signatureFile.write(directory);
        }
        // Write sidecar files
        for (int i = 0; i < sidecarFiles.size(); i++) {
            final int signatureFileIndex = i + 1;
            final String sidecarFilename = String.format(
                    "%s_%d.rcd%s", blockTime().toString().replace(':', '_'), signatureFileIndex, gzipped ? ".gz" : "");
            final Path sidecarFilePath = directory.resolve(sidecarFilename);
            final SidecarFile sidecarFile = sidecarFiles.get(i);
            try (WritableStreamingData wout = new WritableStreamingData(
                    gzipped
                            ? new GZIPOutputStream(Files.newOutputStream(sidecarFilePath))
                            : Files.newOutputStream(sidecarFilePath))) {
                SidecarFile.PROTOBUF.write(sidecarFile, wout);
            }
        }
    }

    /**
     * Validate the parsed record block against the computed previous block hash and the node address book. It checks
     * all the aspects of the record file, signature files, and sidecar files.
     *
     * @param computedPreviousBlockHash the computed previous block hash to compare against
     * @param addressBook the node address book for signature verification
     * @return computed block hash of this record block if valid, exception if invalid
     * @throws ValidationException if a validation error occurs
     */
    public byte[] validate(byte[] computedPreviousBlockHash, NodeAddressBook addressBook) throws ValidationException {
        // Validate the chain, get the previous block hash stored in the record file and compare to computed
        if (!Arrays.equals(recordFile.previousBlockHash(), computedPreviousBlockHash)) {
            throw new ValidationException("Previous block hash does not match computed hash");
        }
        // Validate signature files, this is checking both the signature and computation of signed hash from
        // the file data.
        for (ParsedSignatureFile signatureFile : signatureFiles) {
            if (!signatureFile.isValid(recordFile.signedHash(), addressBook)) {
                throw new ValidationException("Invalid signature file: " + signatureFile.signatureFileName());
            }
        }
        // check we have at least 1/3rd of nodes signatures
        final int totalNodes = addressBook.nodeAddress().size();
        final int validSignatures = signatureFiles.size(); // we know all are valid from above check
        if (validSignatures * 3 < totalNodes) {
            throw new ValidationException("Not enough valid signatures: " + validSignatures + " of " + totalNodes);
        }
        // Validate sidecar files
        MessageDigest digest = sha384Digest();
        for (SidecarFile sidecarFile : sidecarFiles) {
            // compute sidecar file hash
            SidecarFile.PROTOBUF.toBytes(sidecarFile).writeTo(digest);
            final byte[] sidecarHash = digest.digest();
            // check sidecar hash is in record file metadata
            boolean found = false;
            for (var sidecarMetadata : recordFile.recordStreamFile().sidecars()) {
                if (Arrays.equals(sidecarMetadata.hashOrThrow().hash().toByteArray(), sidecarHash)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new ValidationException(
                        "Sidecar file hash not found in record file metadata: " + Arrays.toString(sidecarHash));
            }
        }
        // Recompute record file block hash
        byte[] recomputedBlockHash = recordFile.recomputeBlockHash();
        if (!Arrays.equals(recomputedBlockHash, recordFile.blockHash())) {
            throw new ValidationException("Recomputed block hash does not match stored block hash");
        }
        return recomputedBlockHash;
    }
}
