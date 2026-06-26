// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.verifier;

import static java.lang.System.Logger.Level.WARNING;
import static org.hiero.block.common.hasher.HashingUtilities.hashInternalNode;

import com.hedera.hapi.block.stream.MerklePath;
import com.hedera.hapi.block.stream.SiblingNode;
import com.hedera.hapi.block.stream.StateProof;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import org.hiero.block.node.block.verification.VerificationDataProvider;
import org.hiero.block.node.block.verification.metrics.ProofVerificationMetrics;
import org.hiero.block.node.block.verification.session.SessionFailureType;

/// State proof verifier.
public final class StateProofVerifier implements ProofVerifier {
    private static final System.Logger LOGGER = System.getLogger(StateProofVerifier.class.getName());
    private final ProofVerificationMetrics proofVerificationMetrics;
    private final long blockNumber;
    private final StateProof stateProof;
    private final Bytes rootHash;
    private final VerificationDataProvider verificationDataProvider;

    /// Constructor.
    public StateProofVerifier(
            final ProofVerificationMetrics proofVerificationMetrics,
            final long blockNumber,
            final StateProof stateProof,
            final Bytes rootHash,
            final VerificationDataProvider verificationDataProvider) {
        this.proofVerificationMetrics = Objects.requireNonNull(proofVerificationMetrics);
        this.blockNumber = blockNumber;
        this.stateProof = Objects.requireNonNull(stateProof);
        this.rootHash = Objects.requireNonNull(rootHash);
        this.verificationDataProvider = Objects.requireNonNull(verificationDataProvider);
    }

    /// todo(2879) add documentation
    @Override
    public SessionFailureType verify() {
        final SessionFailureType result;
        final List<MerklePath> paths = stateProof.paths();
        if (paths.size() < 3) {
            LOGGER.log(WARNING, "Block {0} state proof has {1} paths, expected >= 3", blockNumber, paths.size());
            result = SessionFailureType.BAD_BLOCK_PROOF;
        } else {
            final LinkedList<SiblingNode> siblings = new LinkedList<>();

            // walk through the merkle paths unti we find one that is a HASH
            // and the hash (field 3) is equal to our block root, that is the
            // proof item. That will have siblings, so we will combine our
            // block root with the first sibling in the list. Then combine that
            // result with the next sibling until no more siblings. Then check
            // next path index and get that merkle path and continue combining
            // siblings using the siblings list from that new path. Continue
            // following next path index until you reach a merkle path that
            // has next path index == -1. That will represent the signed root.
            // Any given path may be a timestamp leaf. For a timestamp leaf,
            // we hash the content of the timestamp (sha2-384) before combining
            // it with our current hash, and then continue with next index.
            // Remember to keep track of visited indices and if the current
            // path points to the next one which we have already visited, we
            // exit with bad block proof. We should also add the isCanceled boolean
            // here and if we are canceled, we exit with a timeout.

            // MerklePath[6]
            // 0: next == 2, Timestamp=...
            // 1: next == 0, siblings...
            // 2: next == -1, Kind UNSET
            // 3: next == 5, hash == otherHash
            // 4: next == 1, hash == rootHash
            // 5: next == 0, siblings...

            for (final MerklePath merklePath : paths.reversed()) {
                if (merklePath.hasTimestampLeaf()) {
                    final Bytes bytes = merklePath.timestampLeaf();
                }
                for (final SiblingNode siblingNode : merklePath.siblings()) {
                    siblings.add(siblingNode);
                }
            }
            byte[] current = new byte[0];
            for (int i = 0; i < siblings.size(); i++) {
                if (i == 0) {
                    current = combineSibling(rootHash.toByteArray(), siblings.get(i));
                } else {
                    current = combineSibling(current, siblings.get(i));
                }
            }
            final TSSVerifier tssVerifier = new TSSVerifier(
                    proofVerificationMetrics,
                    Bytes.wrap(current),
                    stateProof.signedBlockProof().blockSignature(),
                    verificationDataProvider);
            result = tssVerifier.verify();
        }
        if (result != null) {
            proofVerificationMetrics.stateProofFailure().increment();
        } else {
            proofVerificationMetrics.stateProofSuccess().increment();
        }
        return result;
    }

    private byte[] combineSibling(final byte[] current, final SiblingNode sibling) {
        if (sibling.isLeft()) {
            return hashInternalNode(sibling.hash().toByteArray(), current);
        } else {
            return hashInternalNode(current, sibling.hash().toByteArray());
        }
    }
}
