// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.session.impl;

import static java.lang.System.Logger.Level.INFO;
import static org.hiero.block.common.hasher.HashingUtilities.getBlockItemHash;
import static org.hiero.block.common.hasher.HashingUtilities.noThrowSha384HashOf;

import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.hiero.block.common.hasher.HashingUtilities;
import org.hiero.block.common.hasher.NaiveStreamingTreeHasher;
import org.hiero.block.common.hasher.StreamingTreeHasher;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.verification.session.VerificationSession;

// todo(1661) there is a follow-up task to implement this class based on the expected spec (latest)
public class ExtendedMerkleTreeSession implements VerificationSession {
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    /** The block number being verified. */
    private final long blockNumber;
    // Stream Hashers
    /** The tree hasher for input hashes. */
    private final StreamingTreeHasher inputTreeHasher;
    /** The tree hasher for output hashes. */
    private final StreamingTreeHasher outputTreeHasher;
    /** The tree hasher for consensus header hashes. */
    private final StreamingTreeHasher consensusHeaderHasher;
    /** The tree hasher for state changes hashes. */
    private final StreamingTreeHasher stateChangesHasher;
    /** The tree hasher for trace data hashes. */
    private final StreamingTreeHasher traceDataHasher;
    /** The source of the block, used to construct the final notification. */
    private final BlockSource blockSource;

    /**
     * The block items for the block this session is responsible for. We collect them here so we can provide the
     * complete block in the final notification.
     */
    protected final List<BlockItemUnparsed> blockItems = new ArrayList<>();

    private BlockHeader blockHeader = null;

    private BlockFooter blockFooter = null;

    private List<BlockProof> blockProofs = new ArrayList<>();

    public ExtendedMerkleTreeSession(final long blockNumber, final BlockSource blockSource) {
        this.blockNumber = blockNumber;
        // using NaiveStreamingTreeHasher as we should only need single threaded
        this.inputTreeHasher = new NaiveStreamingTreeHasher();
        this.outputTreeHasher = new NaiveStreamingTreeHasher();
        this.consensusHeaderHasher = new NaiveStreamingTreeHasher();
        this.stateChangesHasher = new NaiveStreamingTreeHasher();
        this.traceDataHasher = new NaiveStreamingTreeHasher();
        this.blockSource = Objects.requireNonNull(blockSource, "BlockSource must not be null");
        LOGGER.log(INFO, "Created ExtendedMerkleTreeSession for block {0}", blockNumber);
    }

    // todo(1661) implement the real logic here, for now just return true if last item has block proof.
    @Override
    public VerificationNotification processBlockItems(List<BlockItemUnparsed> blockItems) throws ParseException {

        // collect block items
        this.blockItems.addAll(blockItems);

        for (BlockItemUnparsed item : blockItems) {
            final BlockItemUnparsed.ItemOneOfType kind = item.item().kind();
            switch (kind) {
                case BLOCK_HEADER -> {
                    this.blockHeader = BlockHeader.PROTOBUF.parse(item.blockHeader());
                    outputTreeHasher.addLeaf(getBlockItemHash(item));
                }
                case ROUND_HEADER, EVENT_HEADER -> consensusHeaderHasher.addLeaf(getBlockItemHash(item));
                case SIGNED_TRANSACTION -> inputTreeHasher.addLeaf(getBlockItemHash(item));
                case TRANSACTION_RESULT, TRANSACTION_OUTPUT -> outputTreeHasher.addLeaf(getBlockItemHash(item));
                case STATE_CHANGES -> stateChangesHasher.addLeaf(getBlockItemHash(item));
                case TRACE_DATA -> traceDataHasher.addLeaf(getBlockItemHash(item));
                // save footer for later
                case BLOCK_FOOTER -> this.blockFooter = BlockFooter.PROTOBUF.parse(item.blockFooter());
                // append block proofs
                case BLOCK_PROOF -> {
                    BlockProof blockProof = BlockProof.PROTOBUF.parse(item.blockProof());
                    blockProofs.add(blockProof);
                }
            }
        }

        // since we are only expecting 1 block proof per block, we can finalize verification here
        // however in the future, we might want to revisit this if we expect multiple proofs
        // and use the EndOfBlock signal to finalize verification.
        if (blockFooter != null && blockProofs.size() > 0) {
            return finalizeVerification(blockProofs.get(0));
        }

        // was not able to finalize verification yet
        return null;
    }

    /**
     * Finalizes the block verification by computing the final block hash,
     * verifying its signature, and updating metrics accordingly.
     *
     * @param blockProof the block proof
     * @return VerificationNotification indicating the result of the verification
     */
    protected VerificationNotification finalizeVerification(BlockProof blockProof) {
        final Bytes blockRootHash = HashingUtilities.computeFinalBlockHash(
                blockHeader,
                blockFooter,
                inputTreeHasher,
                outputTreeHasher,
                consensusHeaderHasher,
                stateChangesHasher,
                traceDataHasher);

        final boolean verified =
                verifySignature(blockRootHash, blockProof.signedBlockProof().blockSignature());
        return new VerificationNotification(
                verified, blockNumber, blockRootHash, verified ? new BlockUnparsed(blockItems) : null, blockSource);
    }

    /**
     * Verifies the signature of a hash, for the dummy implementation this always returns true.
     *
     * @param hash the hash to verify
     * @param signature the signature to verify
     * @return true if the signature is valid, false otherwise
     */
    protected Boolean verifySignature(@NonNull Bytes hash, @NonNull Bytes signature) {
        // TODO we are close to having real TTS signature verification, we maybe should have a config if we are work on
        // TODO preview or production block stream and hence which verification to use

        // Dummy implementation
        // signature = is Hash384( BlockHash )
        return signature.equals(noThrowSha384HashOf(hash));
    }
}
