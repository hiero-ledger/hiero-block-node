// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.verifier;

import static java.lang.System.Logger.Level.WARNING;
import static org.hiero.block.common.hasher.HashingUtilities.hashInternalNode;
import static org.hiero.block.common.hasher.HashingUtilities.hashInternalNodeSingleChild;
import static org.hiero.block.common.hasher.HashingUtilities.hashLeaf;

import com.hedera.hapi.block.stream.MerklePath;
import com.hedera.hapi.block.stream.SiblingNode;
import com.hedera.hapi.block.stream.StateProof;
import com.hedera.pbj.runtime.io.buffer.Bytes;
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

    /// todo(2528) add documentation
    @Override
    public SessionFailureType verify() {
        final SessionFailureType result;
        final List<MerklePath> paths = stateProof.paths();
        if (paths.size() != 3) {
            LOGGER.log(WARNING, "Block {0} state proof has {1} paths, expected 3", blockNumber, paths.size());
            result = SessionFailureType.MALFORMED_PROOF_STRUCTURE;
        } else {
            final MerklePath timestampPath = paths.get(0);
            final MerklePath siblingPath = paths.get(1);
            final MerklePath terminalPath = paths.get(2);
            if (!terminalPath.siblings().isEmpty() || terminalPath.hasTimestampLeaf()) {
                LOGGER.log(WARNING, "Block {0} state proof path 2 (terminal) is unexpectedly non-empty", blockNumber);
                result = SessionFailureType.MALFORMED_PROOF_STRUCTURE;
            } else if (!timestampPath.hasTimestampLeaf()) {
                LOGGER.log(WARNING, "Block {0} state proof path 0 (timestamp) is missing timestamp leaf", blockNumber);
                result = SessionFailureType.MALFORMED_PROOF_STRUCTURE;
            } else if (!stateProof.hasSignedBlockProof()) {
                LOGGER.log(WARNING, "Block {0} state proof is missing signed block proof", blockNumber);
                result = SessionFailureType.MALFORMED_PROOF_STRUCTURE;
            } else {
                final List<SiblingNode> siblings = siblingPath.siblings();
                final int totalSiblings = siblings.size();
                if (totalSiblings < 3 || (totalSiblings - 3) % 4 != 0) {
                    LOGGER.log(
                            WARNING,
                            "Block {0} state proof sibling count {1} is invalid (need >= 3, remainder must be multiple of 4)",
                            blockNumber,
                            totalSiblings);
                    result = SessionFailureType.MALFORMED_PROOF_STRUCTURE;
                } else if (!siblingPath.hasHash() || siblingPath.hash().length() == 0) {
                    LOGGER.log(
                            WARNING,
                            "Block {0} state proof path 1 (sibling) has missing or empty starting hash",
                            blockNumber);
                    result = SessionFailureType.MALFORMED_PROOF_STRUCTURE;
                } else {
                    for (final SiblingNode sibling : siblings) {
                        if (sibling.hash().length() == 0) {
                            LOGGER.log(
                                    WARNING,
                                    "Block {0} state proof contains a sibling node with an empty hash",
                                    blockNumber);
                            proofVerificationMetrics.stateProofFailure().increment();
                            return SessionFailureType.MALFORMED_PROOF_STRUCTURE;
                        }
                    }
                    byte[] current = siblingPath.hash().toByteArray();
                    int index = 0;
                    boolean firstIteration = true;
                    while (totalSiblings - index > 3) {
                        final SiblingNode prevBlockRootsHash = siblings.get(index);
                        final SiblingNode depth5Node2Sibling = siblings.get(index + 1);
                        final SiblingNode depth4Node2Sibling = siblings.get(index + 2);
                        final SiblingNode hashedTimestampSibling = siblings.get(index + 3);
                        final byte[] depth5Node1 = combineSibling(current, prevBlockRootsHash);
                        final byte[] depth4Node1 = combineSibling(depth5Node1, depth5Node2Sibling);
                        final byte[] depth3Node1 = combineSibling(depth4Node1, depth4Node2Sibling);
                        final byte[] depth2Node2 = hashInternalNodeSingleChild(depth3Node1);
                        current = hashInternalNode(hashedTimestampSibling.hash().toByteArray(), depth2Node2);
                        if (firstIteration) {
                            final Bytes reconstructed = Bytes.wrap(current);
                            if (!rootHash.equals(reconstructed)) {
                                LOGGER.log(
                                        WARNING,
                                        "Block {0} state proof integrity check failed: hash reconstructed from path 1 siblings"
                                                + " [{1}] does not match block root hash computed from block content [{2}]",
                                        blockNumber,
                                        reconstructed,
                                        rootHash);
                                proofVerificationMetrics.stateProofFailure().increment();
                                return SessionFailureType.STATE_PROOF_INVALID;
                            }
                            firstIteration = false;
                        }
                        index += 4;
                    }
                    final byte[] depth5Node1 = combineSibling(current, siblings.get(index));
                    final byte[] depth4Node1 = combineSibling(depth5Node1, siblings.get(index + 1));
                    final byte[] depth3Node1 = combineSibling(depth4Node1, siblings.get(index + 2));
                    final byte[] depth2Node2 = hashInternalNodeSingleChild(depth3Node1);
                    final byte[] hashedTimestampLeaf =
                            hashLeaf(timestampPath.timestampLeaf().toByteArray());
                    final byte[] signedBlockRoot = hashInternalNode(hashedTimestampLeaf, depth2Node2);
                    // todo(2528) check if block signature has value else throw missing field
                    final TSSVerifier tssVerifier = new TSSVerifier(
                            proofVerificationMetrics,
                            Bytes.wrap(signedBlockRoot),
                            stateProof.signedBlockProof().blockSignature(),
                            verificationDataProvider);
                    result = tssVerifier.verify();
                }
            }
        }
        if (result != null) {
            proofVerificationMetrics.stateProofFailure().increment();
        } else {
            proofVerificationMetrics.stateProofSuccess().increment();
        }
        return result;
    }

    private byte[] combineSibling(byte[] current, SiblingNode sibling) {
        if (sibling.isLeft()) {
            return hashInternalNode(sibling.hash().toByteArray(), current);
        } else {
            return hashInternalNode(current, sibling.hash().toByteArray());
        }
    }
}
