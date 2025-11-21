// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records.model.parsed;

import com.hedera.hapi.block.stream.experimental.Block;
import com.hedera.hapi.block.stream.experimental.BlockFooter;
import com.hedera.hapi.block.stream.experimental.BlockItem;
import com.hedera.hapi.block.stream.experimental.BlockItem.ItemOneOfType;
import com.hedera.hapi.block.stream.experimental.BlockProof;
import com.hedera.hapi.block.stream.experimental.BlockProof.ProofOneOfType;
import com.hedera.hapi.block.stream.experimental.RecordFileItem;
import com.hedera.hapi.block.stream.experimental.RecordFileSignature;
import com.hedera.hapi.block.stream.experimental.SignedRecordFileProof;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.BlockHashAlgorithm;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.streams.SidecarFile;
import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.time.Instant;
import java.util.List;

/**
 * Converter for converting Hedera record file format blocks into Hedera block stream format blocks and back. The
 * conversion is lossless, you should be able to convert from Record file format block into block stream and back, and
 * all hashes will be the same. So cryptographic verification works on both formats correctly.
 */
public class RecordBlockConverter {

    /**
     * Convert a parsed record file block into a block stream block.
     *
     * @param recordBlock the parsed record file block
     * @param blockNumber the block number for the block which is often not in record files
     * @param rootHashOfBlockHashesMerkleTree the root hash of the block hashes merkle tree, this has to be computed
     *                                        outside this code
     * @return the block stream block
     */
    public static Block toBlock(
            final ParsedRecordBlock recordBlock,
            final long blockNumber,
            final byte[] rootHashOfBlockHashesMerkleTree,
            final NodeAddressBook addressBook) {
        // read the record file into UniversalRecordFile
        final ParsedRecordFile universalRecordFile = recordBlock.recordFile();
        // convert signatures into block proof
        final List<RecordFileSignature> signatures = recordBlock.signatureFiles().stream()
                .parallel()
                // we only include valid signatures in a block proof
                .filter(psf -> psf.isValid(universalRecordFile.blockHash(), addressBook))
                .map(psf -> psf.toRecordFileSignature(addressBook))
                .toList();
        final BlockProof blockProof = new BlockProof(new OneOf<>(
                ProofOneOfType.SIGNED_RECORD_FILE_PROOF,
                new SignedRecordFileProof(universalRecordFile.recordFormatVersion(), signatures)));
        // create a block header
        final Instant blockTime = recordBlock.recordFile().blockTime();
        final Timestamp recordFileTimestamp = new Timestamp(blockTime.getEpochSecond(), blockTime.getNano());
        final BlockHeader blockHeader = new BlockHeader(
                universalRecordFile.hapiProtoVersion(),
                null, // TODO is this right? could be hapi version again, not sure if that is better
                blockNumber,
                recordFileTimestamp, // TODO this needs to be computed based on transaction timestamps
                BlockHashAlgorithm.SHA2_384);
        // create RecordFileItem
        final RecordFileItem recordFileItem = new RecordFileItem(
                recordFileTimestamp, universalRecordFile.recordStreamFile(), recordBlock.sidecarFiles());
        // create footer
        final BlockFooter blockFooter = new BlockFooter(
                Bytes.wrap(recordBlock.recordFile().previousBlockHash()),
                Bytes.wrap(rootHashOfBlockHashesMerkleTree),
                null);
        // create and return the Block
        return new Block(List.of(
                new BlockItem(new OneOf<>(ItemOneOfType.BLOCK_HEADER, blockHeader)),
                new BlockItem(new OneOf<>(ItemOneOfType.RECORD_FILE, recordFileItem)),
                new BlockItem(new OneOf<>(ItemOneOfType.BLOCK_FOOTER, blockFooter)),
                new BlockItem(new OneOf<>(ItemOneOfType.BLOCK_PROOF, blockProof))));
    }

    /**
     * Convert a block stream block into a parsed record file block. This is the round-trip counterpart to toBlock. It
     * is lossless for the record file and sidecar files, but v6 signature files are missing the metadata signatures.
     * The metadata signatures are not included in block stream format as they are not needed as part of the
     * cryptographic trust of the data. The version of the files that come out are the same as the ones that were
     * originally converted to block stream format.
     *
     * @param block the block stream block
     * @param addressBook the node address book for signature verification
     * @return the parsed record file block
     */
    @SuppressWarnings("DataFlowIssue")
    public static ParsedRecordBlock toRecordFile(Block block, final NodeAddressBook addressBook) {
        final RecordFileItem recordFileItem = block.items().stream()
                .filter(BlockItem::hasRecordFile)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Block does not contain a RecordFileItem"))
                .recordFile();
        final BlockHeader blockHeader = block.items().stream()
                .filter(BlockItem::hasBlockHeader)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Block does not contain a BlockHeader"))
                .blockHeader();
        final BlockFooter blockFooter = block.items().stream()
                .filter(BlockItem::hasBlockFooter)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Block does not contain a BlockFooter"))
                .blockFooter();
        final BlockProof blockProof = block.items().stream()
                .filter(BlockItem::hasBlockProof)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Block does not contain a BlockProof"))
                .blockProof();
        final SignedRecordFileProof signedRecordFileProof = blockProof.signedRecordFileProof();
        final Instant blockTime = Instant.ofEpochSecond(
                recordFileItem.creationTime().seconds(),
                recordFileItem.creationTime().nanos());
        final ParsedRecordFile recordFile = ParsedRecordFile.parse(
                blockTime,
                signedRecordFileProof.version(),
                blockHeader.hapiProtoVersion(),
                blockFooter.previousBlockRootHash().toByteArray(),
                recordFileItem.recordFileContents());
        List<ParsedSignatureFile> signatureFiles = signedRecordFileProof.recordFileSignatures().stream()
                .map(rfs -> new ParsedSignatureFile(
                        rfs.signaturesBytes().toByteArray(),
                        rfs.nodeId(),
                        signedRecordFileProof.version(),
                        blockTime,
                        recordFile.signedHash(),
                        addressBook))
                .toList();
        List<SidecarFile> sidecarFiles = recordFileItem.sidecarFileContents();
        return new ParsedRecordBlock(recordFile, signatureFiles, sidecarFiles);
    }
}
