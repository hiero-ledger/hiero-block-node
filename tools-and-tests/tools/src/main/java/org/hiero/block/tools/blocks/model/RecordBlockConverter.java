package org.hiero.block.tools.blocks.model;

import com.hedera.hapi.block.stream.experimental.Block;
import com.hedera.hapi.block.stream.experimental.BlockFooter;
import com.hedera.hapi.block.stream.experimental.BlockItem;
import com.hedera.hapi.block.stream.experimental.BlockItem.ItemOneOfType;
import com.hedera.hapi.block.stream.experimental.BlockProof;
import com.hedera.hapi.block.stream.experimental.BlockProof.ProofOneOfType;
import com.hedera.hapi.block.stream.experimental.RecordFileSignature;
import com.hedera.hapi.block.stream.experimental.SignedRecordFileProof;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.BlockHashAlgorithm;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import org.hiero.block.tools.records.InMemoryFile;
import org.hiero.block.tools.records.ParsedSignatureFile;
import org.hiero.block.tools.records.RecordFileBlock;
import org.hiero.block.tools.records.UniversalRecordFile;

/**
 * Converter for converting Hedera record file format blocks into Hedera block stream format blocks and back. The
 * conversion is lossless, you should be able to convert from Record file format block into block stream and back, and
 * all hashes will be the same. So cryptographic verification works on both formats correctly.
 */
public class RecordBlockConverter {

    public static Block toBlock(
            final InMemoryFile recordFile,
            final List<InMemoryFile> signatureFiles,
            final List<InMemoryFile> primarySidecarFiles,
            final long blockNumber,
            final Instant blockTime,
            final byte[] previousBlockHash,
            final byte[] rootHashOfBlockHashesMerkleTree,
            final NodeAddressBook addressBook) {
        // read the record file into UniversalRecordFile
        final UniversalRecordFile universalRecordFile = UniversalRecordFile.parse(recordFile.data(), blockNumber);
        // convert signatures into block proof
        final List<RecordFileSignature> signatures = signatureFiles.stream()
            .parallel()
            .map(sf -> new ParsedSignatureFile(addressBook, sf))
            .filter(psf -> psf.isValid(universalRecordFile.blockHash()))
            .map(ParsedSignatureFile::toRecordFileSignature)
            .toList();
        final BlockProof blockProof = new BlockProof(
            new OneOf<>(ProofOneOfType.SIGNED_RECORD_FILE_PROOF, new SignedRecordFileProof(
                universalRecordFile.recordFormatVersion(), signatures)));
        // create a block header
        final BlockHeader blockHeader = new BlockHeader(
            universalRecordFile.hapiProtoVersion(),
            universalRecordFile.hapiProtoVersion(), // TODO is this right? could be unset, not if that is better
            blockNumber,
            new Timestamp(
                blockTime.getEpochSecond(), blockTime.getNano()), // TODO is the the right time to use?
            BlockHashAlgorithm.SHA2_384);
        // create footer
        final BlockFooter blockFooter = new BlockFooter(
            Bytes.wrap(previousBlockHash),
            Bytes.wrap(rootHashOfBlockHashesMerkleTree),
            null);
        // create and return the Block
        return new Block(List.of(
            new BlockItem(new OneOf<>(ItemOneOfType.BLOCK_HEADER, blockHeader)),
            new BlockItem(new OneOf<>(ItemOneOfType.RECORD_FILE, universalRecordFile.recordStreamFile())),
            new BlockItem(new OneOf<>(ItemOneOfType.BLOCK_FOOTER, blockFooter)),
            new BlockItem(new OneOf<>(ItemOneOfType.BLOCK_PROOF, blockProof))));
    }

    public static RecordFileBlock toRecordFile(Block block) {
        return null;
    }
}
