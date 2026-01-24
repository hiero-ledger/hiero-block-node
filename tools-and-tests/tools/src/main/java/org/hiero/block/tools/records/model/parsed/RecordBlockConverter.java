// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records.model.parsed;

import static org.hiero.block.tools.utils.Sha384.ZERO_HASH;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.BlockItem.ItemOneOfType;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.BlockProof.ProofOneOfType;
import com.hedera.hapi.block.stream.RecordFileItem;
import com.hedera.hapi.block.stream.RecordFileSignature;
import com.hedera.hapi.block.stream.SignedRecordFileProof;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.BlockHashAlgorithm;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.streams.RecordStreamItem;
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
     * @param previousBlockStreamBlockHash the previous block stream block hash, this has to be computed outside
     * @param rootHashOfBlockHashesMerkleTree the root hash of the block hashes merkle tree, this has to be computed
     *                                        outside this code
     * @return the block stream block
     */
    public static Block toBlock(
            final ParsedRecordBlock recordBlock,
            final long blockNumber,
            final byte[] previousBlockStreamBlockHash,
            final byte[] rootHashOfBlockHashesMerkleTree,
            final NodeAddressBook addressBook) {
        // read the record file into UniversalRecordFile
        final ParsedRecordFile universalRecordFile = recordBlock.recordFile();
        // convert signatures into block proof
        final List<RecordFileSignature> signatures = recordBlock.signatureFiles().stream()
                .parallel()
                // we only include valid signatures in a block proof
                .filter(psf -> psf.isValid(universalRecordFile.signedHash(), addressBook))
                .map(psf -> psf.toRecordFileSignature(addressBook))
                .toList();
        final BlockProof blockProof = new BlockProof(
                blockNumber,
                new OneOf<>(
                        ProofOneOfType.SIGNED_RECORD_FILE_PROOF,
                        new SignedRecordFileProof(universalRecordFile.recordFormatVersion(), signatures)));
        // create a block header
        final Instant blockTime = recordBlock.recordFile().blockTime();
        final Timestamp recordFileTimestamp = new Timestamp(blockTime.getEpochSecond(), blockTime.getNano());
        final Timestamp firstTransactionTimestamp =
                recordBlock.recordFile().recordStreamFile().recordStreamItems().stream()
                        .filter(RecordStreamItem::hasRecord)
                        .map(rsi -> rsi.recordOrThrow().consensusTimestampOrThrow())
                        .findFirst()
                        .orElseThrow();
        final BlockHeader blockHeader = new BlockHeader(
                universalRecordFile.hapiProtoVersion(),
                // There is no software version in the record files, so setting to `null`. Ideally, we would track down
                // this data. The best idea how to do that is to use mirror node data to work out consensus times of
                // network upgrades. Then take those times, look up archived saved states backups in the bucket. Then
                // try and read the first saved saved-state (many custom binary formats) after each upgrade. From there
                // we could in theory work out a software version they were written with.
                null,
                blockNumber,
                // block time is the time of the first round that is the time of the first transaction, this is
                // different from the record file time
                firstTransactionTimestamp,
                BlockHashAlgorithm.SHA2_384);
        // create RecordFileItem
        final RecordFileItem recordFileItem = new RecordFileItem(
                recordFileTimestamp, universalRecordFile.recordStreamFile(), recordBlock.sidecarFiles());
        // create footer
        final BlockFooter blockFooter = new BlockFooter(
                // the hash chain between block stream blocks has to use block stream hashes
                Bytes.wrap(previousBlockStreamBlockHash),
                Bytes.wrap(rootHashOfBlockHashesMerkleTree),
                // the state root hash for wrapped blocks is 48 zeros to indicate there is NO HASH
                Bytes.wrap(ZERO_HASH));
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
                recordFileItem
                        .recordFileContents()
                        .startObjectRunningHash()
                        .hash()
                        .toByteArray(),
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
