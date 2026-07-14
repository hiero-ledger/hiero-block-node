// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.verifier;

import static java.lang.System.Logger.Level.WARNING;
import static org.hiero.block.common.hasher.HashingUtilities.hashInternalNode;
import static org.hiero.block.common.hasher.HashingUtilities.hashInternalNodeSingleChild;
import static org.hiero.block.common.hasher.HashingUtilities.hashLeaf;

import com.hedera.hapi.block.stream.MerklePath;
import com.hedera.hapi.block.stream.MerklePath.ContentOneOfType;
import com.hedera.hapi.block.stream.SiblingNode;
import com.hedera.hapi.block.stream.StateProof;
import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.lang.System.Logger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.hiero.block.node.block.verification.VerificationDataProvider;
import org.hiero.block.node.block.verification.metrics.ProofVerificationMetrics;
import org.hiero.block.node.block.verification.session.SessionFailureType;

/// State proof verifier.
public final class StateProofVerifier implements ProofVerifier {
    private static final Logger LOGGER = System.getLogger(StateProofVerifier.class.getName());
    private final AtomicBoolean isCanceled;
    private final ProofVerificationMetrics proofVerificationMetrics;
    private final long blockNumber;
    private final StateProof stateProof;
    private final Bytes rootHash;
    private final VerificationDataProvider verificationDataProvider;
    private final HashMap<Integer, MergeCheckpoint> checkpoints;
    private boolean foundComputedRootHashMatch = false;
    private byte[] signedBlockRoot = null;

    /// Constructor.
    public StateProofVerifier(
            final AtomicBoolean isCanceled,
            final ProofVerificationMetrics proofVerificationMetrics,
            final long blockNumber,
            final StateProof stateProof,
            final Bytes rootHash,
            final VerificationDataProvider verificationDataProvider) {
        this.isCanceled = Objects.requireNonNull(isCanceled);
        this.proofVerificationMetrics = Objects.requireNonNull(proofVerificationMetrics);
        this.blockNumber = blockNumber;
        this.stateProof = Objects.requireNonNull(stateProof);
        this.rootHash = Objects.requireNonNull(rootHash);
        this.verificationDataProvider = Objects.requireNonNull(verificationDataProvider);
        this.checkpoints = new HashMap<>();
    }

    /// todo(2879) add documentation
    @Override
    public SessionFailureType verify() {
        final SessionFailureType result;
        final List<MerklePath> allPaths = stateProof.paths();
        if (allPaths.size() < 3) {
            LOGGER.log(WARNING, "Block {0} state proof has {1} paths, expected >= 3", blockNumber, allPaths.size());
            result = SessionFailureType.BAD_BLOCK_PROOF;
        } else if (!isLeaf(allPaths.getFirst())) {
            LOGGER.log(WARNING, "Block {0} state proof has non-leaf first path", blockNumber);
            result = SessionFailureType.BAD_BLOCK_PROOF;
        } else {
            // Iterate over all paths, processing leafs in the order they are encountered until we have reconstructed
            // the signed block root.
            for (int leafStartingIndex = 0; leafStartingIndex < allPaths.size(); leafStartingIndex++) {
                if (signedBlockRoot != null) {
                    // If we have set the signedBlockRoot, this means that we can now complete verification
                    break;
                } else if (isCanceled()) {
                    return SessionFailureType.CANCELLED;
                } else {
                    final MerklePath currentPath = allPaths.get(leafStartingIndex);
                    if (isLeaf(currentPath)) {
                        // First process the found leaf
                        final byte[] leafResult = processLeaf(currentPath);
                        // Now we must follow the next index, which must always be a join point,
                        // and we keep following that while we can
                        final SessionFailureType followPathResult =
                                followNextPath(currentPath, allPaths, leafStartingIndex, leafResult);
                        if (followPathResult != null) {
                            return followPathResult;
                        }
                    }
                }
            }
            // After we have finished iterating over all paths, now we can complete verification
            result = completeVerification(signedBlockRoot, foundComputedRootHashMatch, stateProof);
        }
        if (result != null) {
            proofVerificationMetrics.stateProofFailure().increment();
        } else {
            proofVerificationMetrics.stateProofSuccess().increment();
        }
        return result;
    }

    /// Process a leaf by computing its content first and then merging all siblings.
    /// This result will then be used as the content to continue to the next index of the leaf.
    private byte[] processLeaf(final MerklePath leaf) {
        final byte[] leafContent = computeLeafContent(leaf);
        return mergeSiblings(leaf, leafContent);
    }

    /// Compute the content of a leaf.
    private byte[] computeLeafContent(final MerklePath leaf) {
        return switch (leaf.content().kind()) {
            case UNSET -> throw new IllegalArgumentException("Unexpected MerklePath content kind");
            case HASH -> {
                final byte[] hash = leaf.hash().toByteArray();
                if (!foundComputedRootHashMatch) {
                    foundComputedRootHashMatch = Arrays.equals(rootHash.toByteArray(), hash);
                }
                yield hash;
            }
            case STATE_ITEM_LEAF -> hashLeaf(leaf.stateItemLeaf().toByteArray());
            case BLOCK_ITEM_LEAF -> hashLeaf(leaf.blockItemLeaf().toByteArray());
            case TIMESTAMP_LEAF -> hashLeaf(leaf.timestampLeaf().toByteArray());
        };
    }

    /// Follow the next path index of a leaf while we can, while merging or creating checkpoints for join points.
    /// This method can return a non-null [SessionFailureType] in case we can no longer continue the
    /// verification process, else `null` will be returned so we can continue with the next found leaf.
    /// If we have reached the final join, the [#signedBlockRoot] will be set, which also flags that we can now
    /// proceed to complete the verification.
    private SessionFailureType followNextPath(
            final MerklePath currentPath,
            final List<MerklePath> allPaths,
            final int leafStartingIndex,
            final byte[] leafResult) {
        byte[] currentResult = leafResult;
        int lowestStartingIndex = leafStartingIndex;
        int currentJoinPointIndex = currentPath.nextPathIndex();
        boolean canFollowNextIndex = true;
        do {
            if (isCanceled()) {
                return SessionFailureType.CANCELLED;
            } else if (currentJoinPointIndex < 0) {
                signedBlockRoot = currentResult;
                canFollowNextIndex = false;
            } else if (currentJoinPointIndex >= allPaths.size()) {
                return SessionFailureType.BAD_BLOCK_PROOF;
            } else {
                final MerklePath nextPath = allPaths.get(currentJoinPointIndex);
                if (isJoinPoint(nextPath)) {
                    final MergeCheckpoint checkpoint = checkpoints.remove(currentJoinPointIndex);
                    if (checkpoint != null) {
                        // now we need to merge this with the checkpoint
                        if (checkpoint.startIndex == leafStartingIndex
                                || checkpoint.startIndex == lowestStartingIndex) {
                            // This is not expected to happen
                            LOGGER.log(
                                    WARNING,
                                    "Found duplicate checkpoint at index {0} for block {1}",
                                    currentJoinPointIndex,
                                    blockNumber);
                            return SessionFailureType.BAD_BLOCK_PROOF;
                        } else if (checkpoint.startIndex < lowestStartingIndex) {
                            currentResult = hashInternalNode(checkpoint.currentHash, currentResult);
                            lowestStartingIndex = checkpoint.startIndex;
                        } else {
                            currentResult = hashInternalNode(currentResult, checkpoint.currentHash);
                        }
                        currentResult = mergeSiblings(nextPath, currentResult);
                    } else {
                        this.checkpoints.put(
                                currentJoinPointIndex, new MergeCheckpoint(lowestStartingIndex, currentResult));
                        canFollowNextIndex = false;
                    }
                    currentJoinPointIndex = nextPath.nextPathIndex();
                } else {
                    return SessionFailureType.BAD_BLOCK_PROOF;
                }
            }
        } while (canFollowNextIndex);
        return null;
    }

    /// Complete verification by verifying the signed block root.
    private SessionFailureType completeVerification(
            final byte[] signedBlockRoot, final boolean foundComputedRootHashMatch, final StateProof stateProof) {
        final SessionFailureType result;
        if (signedBlockRoot == null || !foundComputedRootHashMatch) {
            result = SessionFailureType.BAD_BLOCK_PROOF;
        } else {
            if (stateProof.hasSignedBlockProof()) {
                final Bytes signature = stateProof.signedBlockProof().blockSignature();
                if (signature == null || signature.equals(Bytes.EMPTY)) {
                    result = SessionFailureType.BAD_BLOCK_PROOF;
                } else {
                    final TSSVerifier tssVerifier = new TSSVerifier(
                            proofVerificationMetrics, Bytes.wrap(signedBlockRoot), signature, verificationDataProvider);
                    result = tssVerifier.verify();
                }
            } else {
                result = SessionFailureType.BAD_BLOCK_PROOF;
            }
        }
        return result;
    }

    /// Merge all available siblings of a [MerklePath] with a provided content.
    private byte[] mergeSiblings(final MerklePath path, final byte[] content) {
        byte[] result = content;
        for (final SiblingNode sibling : path.siblings()) {
            if (sibling.hash() == null || sibling.hash().equals(Bytes.EMPTY)) {
                result = hashInternalNodeSingleChild(result);
            } else {
                result = combineSibling(result, sibling);
            }
        }
        return result;
    }

    /// Checks if a given [MerklePath] is a leaf. A merkle path is a leaf if
    /// it has content.
    private boolean isLeaf(final MerklePath path) {
        return hasContent(path);
    }

    /// Checks if a given [MerklePath] is a join point. A merkle path is a join
    /// point if it has no content.
    private boolean isJoinPoint(final MerklePath path) {
        return !hasContent(path);
    }

    /// Checks if a given [MerklePath] has content.
    private boolean hasContent(final MerklePath path) {
        final OneOf<ContentOneOfType> content = path.content();
        return content != null
                && content.kind() != ContentOneOfType.UNSET
                && content.value() != null
                && content.value() != Bytes.EMPTY;
    }

    /// Combine a sibling node with the provided content.
    private byte[] combineSibling(final byte[] content, final SiblingNode sibling) {
        if (sibling.isLeft()) {
            return hashInternalNode(sibling.hash().toByteArray(), content);
        } else {
            return hashInternalNode(content, sibling.hash().toByteArray());
        }
    }

    private boolean isCanceled() {
        return isCanceled.get() || Thread.currentThread().isInterrupted();
    }

    /// A simple record that contains data for a merge checkpoint, i.e., a join point.
    /// To be used within the [#checkpoints] map, where the key of the entry is
    /// the actual join point index and the value is this record which holds the start index
    /// and the current result hash.
    /// @param startIndex the lowest index from which this point originated
    /// @param currentHash the hash computed at this checkpoint, to be used to merge with a sibling.
    private record MergeCheckpoint(int startIndex, byte[] currentHash) {}
}
