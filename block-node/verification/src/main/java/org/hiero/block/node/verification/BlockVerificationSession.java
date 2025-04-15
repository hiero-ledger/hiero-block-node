// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification;

import static org.hiero.block.common.hasher.HashingUtilities.getBlockItemHash;

import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.List;
import org.hiero.block.common.hasher.HashingUtilities;
import org.hiero.block.common.hasher.NaiveStreamingTreeHasher;
import org.hiero.block.common.hasher.StreamingTreeHasher;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.hapi.block.node.BlockItemUnparsed;
import org.hiero.hapi.block.node.BlockUnparsed;

/**
 * Block verification for a single block, aka. session. A new one is created for each block to verify. This is a simple
 * separate class so it is easy to test.
 */
public class BlockVerificationSession {
    /** The block number being verified. */
    protected final long blockNumber;
    /** The tree hasher for input hashes. */
    protected final StreamingTreeHasher inputTreeHasher;
    /** The tree hasher for output hashes. */
    protected final StreamingTreeHasher outputTreeHasher;
    /**
     * The block items for the block this session is responsible for. We collect them here so we can provide the
     * complete block in the final notification.
     */
    protected final List<BlockItemUnparsed> blockItems = new ArrayList<>();

    /**
     * Constructs the session with shared initialization logic.
     *
     * @param blockNumber the block number to verify, we pass it in even though we could extract from block items to
     *                    avoid having to duplicate parsing work of the block header.
     */
    protected BlockVerificationSession(final long blockNumber) {
        this.blockNumber = blockNumber;
        // using NaiveStreamingTreeHasher as we should only need single threaded
        this.inputTreeHasher = new NaiveStreamingTreeHasher();
        this.outputTreeHasher = new NaiveStreamingTreeHasher();
    }

    /**
     * Processes the provided block items by updating the tree hashers.
     * If the last item has a block proof, final verification is triggered.
     *
     * @param blockItems the block items to process
     * @return VerificationNotification indicating the result of the verification if these items included the final
     *          block proof otherwise null
     * @throws ParseException if a parsing error occurs
     */
    public VerificationNotification processBlockItems(List<BlockItemUnparsed> blockItems) throws ParseException {
        // Collect the block items for later use in producing the block notification
        this.blockItems.addAll(blockItems);
        // branch based on the type of block item and update respective merkle tree
        for (BlockItemUnparsed item : blockItems) {
            final BlockItemUnparsed.ItemOneOfType kind = item.item().kind();
            switch (kind) {
                case EVENT_HEADER, EVENT_TRANSACTION, ROUND_HEADER -> inputTreeHasher.addLeaf(getBlockItemHash(item));
                case TRANSACTION_OUTPUT, STATE_CHANGES, TRANSACTION_RESULT, BLOCK_HEADER -> outputTreeHasher.addLeaf(
                        getBlockItemHash(item));
            }
        }
        // Check if this batch contains the final block proof
        final BlockItemUnparsed lastItem = blockItems.getLast();
        if (lastItem.hasBlockProof()) {
            @SuppressWarnings("DataFlowIssue")
            BlockProof blockProof = BlockProof.PROTOBUF.parse(lastItem.blockProof());
            return finalizeVerification(blockProof);
        }
        // was not the last item, so we are not done yet
        return null;
    }

    /**
     * Finalizes the block verification by computing the final block hash,
     * verifying its signature, and updating metrics accordingly.
     *
     * @param blockProof the block proof
     * @return VerificationNotification indicating the result of the verification
     */
    VerificationNotification finalizeVerification(BlockProof blockProof) {
        final Bytes blockHash = HashingUtilities.computeFinalBlockHash(blockProof, inputTreeHasher, outputTreeHasher);
        final boolean verified = verifySignature(blockHash, blockProof.blockSignature());
        return new VerificationNotification(
                verified, blockNumber, blockHash, verified ? new BlockUnparsed(blockItems) : null);
    }

    /**
     * Verifies the signature of a hash, for the dummy implementation this always returns true.
     *
     * @param hash the hash to verify
     * @param signature the signature to verify
     * @return true if the signature is valid, false otherwise
     */
    Boolean verifySignature(@NonNull Bytes hash, @NonNull Bytes signature) {
        // TODO we are close to having real TTS signature verification, we maybe should have a config if we are work on
        // TODO preview or production block stream and hence which verification to use

        // Dummy implementation
        // signature = is Hash384( BlockHash )
        return signature.equals(HashingUtilities.noThrowSha384HashOf(hash));
    }
}
