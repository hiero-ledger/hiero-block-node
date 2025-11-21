// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records.model.parsed;

import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.streams.SidecarFile;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.WritableSequentialData;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
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
                        .map(ParsedSignatureFile::new)
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
            final int signatureFileIndex = i+1;
            final String sidecarFilename = String.format(
                    "%s_%d.rcd%s",
                    blockTime().toString().replace(':', '_'),
                    signatureFileIndex,
                    gzipped ? ".gz" : "");
            final Path sidecarFilePath = directory.resolve(sidecarFilename);
            final SidecarFile sidecarFile = sidecarFiles.get(i);
            try(WritableStreamingData wout = new WritableStreamingData(gzipped ?
                new GZIPOutputStream(Files.newOutputStream(sidecarFilePath)) :
                    Files.newOutputStream(sidecarFilePath))) {
                    SidecarFile.PROTOBUF.write(sidecarFile, wout);
            }
        }
    }

    public boolean validate(byte[] previousBlockHash, NodeAddressBook addressBook) {
        return false;
    }
}
