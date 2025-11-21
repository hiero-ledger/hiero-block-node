// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records.model.parsed;

import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.streams.SidecarFile;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
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
        recordFile.write(directory, gzipped);
    }

    public boolean validate(byte[] previousBlockHash, NodeAddressBook addressBook) {
        return false;
    }
}
