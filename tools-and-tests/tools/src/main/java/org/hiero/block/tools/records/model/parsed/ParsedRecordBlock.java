// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records.model.parsed;

import static org.hiero.block.tools.utils.Sha384.sha384Digest;

import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.streams.SidecarFile;
import com.hedera.hapi.streams.SidecarMetadata;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
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

    /** Maximum depth for parsing nested messages (PBJ default). */
    private static final int MAX_DEPTH = 512;

    /** Maximum size for parsing sidecar files (8MB). Some mainnet blocks have large sidecars. */
    private static final int MAX_SIDECAR_SIZE = 8 * 1024 * 1024;

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
     * <p>Sidecar candidates picked up from the source (any file matching the {@code _NN.rcd}
     * sidecar naming pattern next to the record file) are filtered against the record file's
     * signed {@code sidecars[]} manifest. Any candidate whose SHA-384 does not appear in the
     * signed manifest is dropped with a warning: the record file's manifest is authoritative,
     * and embedding a candidate the CN never signed produces a wrapped block with unsigned
     * bytes that downstream integrity checks will reject. See issue #3224 for background.
     *
     * @param recordFileBlock the unparsed record block
     * @return the parsed record block
     */
    public static ParsedRecordBlock parse(UnparsedRecordBlock recordFileBlock) {
        final ParsedRecordFile recordFile = ParsedRecordFile.parse(recordFileBlock.primaryRecordFile());
        final List<ParsedSignatureFile> signatureFiles = recordFileBlock.signatureFiles().stream()
                .map(sigFile -> {
                    try {
                        return new ParsedSignatureFile(sigFile);
                    } catch (RuntimeException e) {
                        System.err.println("Warning: Skipping corrupted signature file: " + sigFile.path() + " ("
                                + sigFile.data().length + " bytes) - " + e.getMessage());
                        return null;
                    }
                })
                .filter(sig -> sig != null)
                .toList();
        final List<SidecarFile> parsedSidecars = recordFileBlock.primarySidecarFiles().stream()
                .map(ps -> {
                    try {
                        // Use 5-arg parse to specify both maxDepth and maxSize
                        // The 3-arg parse incorrectly passes maxSize as maxDepth
                        return SidecarFile.PROTOBUF.parse(
                                Bytes.wrap(ps.data()).toReadableSequentialData(),
                                false, // strictMode
                                true, // parseUnknownFields
                                MAX_DEPTH, // maxDepth
                                MAX_SIDECAR_SIZE); // maxSize
                    } catch (ParseException e) {
                        throw new RuntimeException(e);
                    }
                })
                .toList();
        final List<SidecarFile> filteredSidecars = filterOrphanSidecars(
                parsedSidecars, recordFile.recordStreamFile().sidecars());
        return new ParsedRecordBlock(recordFile, signatureFiles, filteredSidecars);
    }

    /**
     * Filter a list of parsed sidecar files, keeping only those whose SHA-384 appears in the
     * record file's signed sidecar manifest. Any sidecar without a matching signed hash is
     * dropped with a warning printed to {@code stderr}.
     *
     * <p>Motivation: at wrap time, sidecar candidates are gathered by filename pattern next to
     * the record file. In rare cases a single dissenting CN node writes a sidecar file for a
     * block that consensus later declared had no sidecar; that orphan lingers in the bucket
     * and gets swept in by the filename matcher. Embedding it into the WRB produces bytes with
     * no cryptographic backing. Since the RSA signature only covers the manifest, treating the
     * manifest as authoritative and dropping unmatched candidates keeps the WRB honest.
     *
     * <p>The returned list preserves the relative order of the retained sidecars. This is
     * package-private so tests can exercise the filter directly.
     */
    static List<SidecarFile> filterOrphanSidecars(
            final List<SidecarFile> parsedSidecars, final List<SidecarMetadata> sidecarMetadatas) {
        return filterOrphanSidecars(parsedSidecars, sidecarMetadatas, System.err::println);
    }

    /**
     * Overload of {@link #filterOrphanSidecars(List, List)} with an injectable warning sink so
     * tests can capture drop diagnostics without racing on {@code System.setErr}. The sink is
     * invoked once per dropped sidecar with a human-readable message.
     */
    static List<SidecarFile> filterOrphanSidecars(
            final List<SidecarFile> parsedSidecars,
            final List<SidecarMetadata> sidecarMetadatas,
            final java.util.function.Consumer<String> warningSink) {
        if (parsedSidecars.isEmpty()) {
            return parsedSidecars;
        }
        final List<byte[]> signedHashes = new ArrayList<>(sidecarMetadatas.size());
        for (final SidecarMetadata meta : sidecarMetadatas) {
            if (meta.hasHash()) {
                signedHashes.add(meta.hashOrThrow().hash().toByteArray());
            }
        }
        final MessageDigest digest = sha384Digest();
        final List<SidecarFile> kept = new ArrayList<>(parsedSidecars.size());
        for (int i = 0; i < parsedSidecars.size(); i++) {
            final SidecarFile sf = parsedSidecars.get(i);
            SidecarFile.PROTOBUF.toBytes(sf).writeTo(digest);
            final byte[] hash = digest.digest();
            if (containsHash(signedHashes, hash)) {
                kept.add(sf);
            } else {
                warningSink.accept("Warning: Dropping orphan sidecar #" + i + " (SHA-384 " + hex(hash)
                        + ") not referenced by record file's signed sidecars[] manifest");
            }
        }
        return kept;
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
