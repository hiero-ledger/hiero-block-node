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
import java.util.Map;
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
    boolean foundComputedRootHashMatch = false;

    /// Constructor.
    public StateProofVerifier(
            final AtomicBoolean isCanceled,
            final ProofVerificationMetrics proofVerificationMetrics,
            final long blockNumber,
            final StateProof stateProof,
            final Bytes rootHash,
            final VerificationDataProvider verificationDataProvider) {
        this.isCanceled = isCanceled;
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
        } else if (!isLeaf(paths.getFirst())) {
            LOGGER.log(WARNING, "Block {0} state proof has non-leaf first path", blockNumber);
            result = SessionFailureType.BAD_BLOCK_PROOF;
        } else {
            final Map<Integer, MergeCheckpoint> checkpoints = new HashMap<>();
            byte[] signedBlockRoot = null;
            for (int leafStartingIndex = 0; leafStartingIndex < paths.size(); leafStartingIndex++) {
                if (signedBlockRoot != null) {
                    break;
                } else if (isCanceled()) {
                    return SessionFailureType.CANCELLED;
                } else {
                    final MerklePath path = paths.get(leafStartingIndex);
                    if (isLeaf(path)) {
                        // First process the next found leaf
                        byte[] pathResult =
                                switch (path.content().kind()) {
                                    case UNSET ->
                                        throw new IllegalArgumentException("Unexpected MerklePath content kind");
                                    case HASH -> {
                                        final byte[] hash = path.hash().toByteArray();
                                        if (!foundComputedRootHashMatch) {
                                            foundComputedRootHashMatch = Arrays.equals(rootHash.toByteArray(), hash);
                                        }
                                        yield hash;
                                    }
                                    case STATE_ITEM_LEAF ->
                                        hashLeaf(path.stateItemLeaf().toByteArray());
                                    case BLOCK_ITEM_LEAF ->
                                        hashLeaf(path.blockItemLeaf().toByteArray());
                                    case TIMESTAMP_LEAF ->
                                        hashLeaf(path.timestampLeaf().toByteArray());
                                };
                        pathResult = mergeSiblings(path, pathResult);
                        // Now we must follow the next index, which must always be a join point, and
                        // we keep following that until we can
                        boolean canFollowNextIndex = true;
                        int currentJoinPointIndex = path.nextPathIndex();
                        while (canFollowNextIndex) {
                            if (isCanceled()) {
                                return SessionFailureType.CANCELLED;
                            } else if (currentJoinPointIndex < 0) {
                                signedBlockRoot = pathResult;
                                canFollowNextIndex = false;
                            } else if (currentJoinPointIndex >= paths.size()) {
                                return SessionFailureType.BAD_BLOCK_PROOF;
                            } else {
                                final MerklePath nextPath = paths.get(currentJoinPointIndex);
                                if (isJoinPoint(nextPath)) {
                                    final MergeCheckpoint checkpoint = checkpoints.remove(currentJoinPointIndex);
                                    if (checkpoint != null) {
                                        // now we need to merge this with the checkpoint
                                        if (checkpoint.startIndex == leafStartingIndex) {
                                            return SessionFailureType.BAD_BLOCK_PROOF;
                                        } else if (checkpoint.startIndex < leafStartingIndex) {
                                            pathResult = hashInternalNode(checkpoint.currentHash, pathResult);
                                        } else {
                                            pathResult = hashInternalNode(pathResult, checkpoint.currentHash);
                                        }
                                        pathResult = mergeSiblings(nextPath, pathResult);
                                    } else {
                                        checkpoints.put(
                                                currentJoinPointIndex,
                                                new MergeCheckpoint(leafStartingIndex, pathResult));
                                        canFollowNextIndex = false;
                                    }
                                    currentJoinPointIndex = nextPath.nextPathIndex();
                                } else {
                                    return SessionFailureType.BAD_BLOCK_PROOF;
                                }
                            }
                        }
                    }
                }
            }
            if (signedBlockRoot == null || !foundComputedRootHashMatch) {
                result = SessionFailureType.BAD_BLOCK_PROOF;
            } else {
                if (stateProof.hasSignedBlockProof()) {
                    final TSSVerifier tssVerifier = new TSSVerifier(
                            proofVerificationMetrics,
                            Bytes.wrap(signedBlockRoot),
                            stateProof.signedBlockProof().blockSignature(),
                            verificationDataProvider);
                    result = tssVerifier.verify();
                } else {
                    result = SessionFailureType.BAD_BLOCK_PROOF;
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

    private byte[] mergeSiblings(final MerklePath path, final byte[] pathResult) {
        byte[] result = pathResult;
        for (final SiblingNode sibling : path.siblings()) {
            if (sibling.hash() == null || sibling.hash().equals(Bytes.EMPTY)) {
                result = hashInternalNodeSingleChild(result);
            } else {
                result = combineSibling(result, sibling);
            }
        }
        return result;
    }

    private boolean isLeaf(final MerklePath path) {
        return hasContent(path);
    }

    private boolean isJoinPoint(final MerklePath path) {
        return !hasContent(path) && !hasSiblings(path);
    }

    private boolean hasSiblings(final MerklePath path) {
        return !path.siblings().isEmpty();
    }

    private boolean hasContent(final MerklePath path) {
        final OneOf<ContentOneOfType> content = path.content();
        return content != null
                && content.kind() != ContentOneOfType.UNSET
                && content.value() != null
                && content.value() != Bytes.EMPTY;
    }

    private byte[] combineSibling(final byte[] current, final SiblingNode sibling) {
        if (sibling.isLeft()) {
            return hashInternalNode(sibling.hash().toByteArray(), current);
        } else {
            return hashInternalNode(current, sibling.hash().toByteArray());
        }
    }

    private boolean isCanceled() {
        return isCanceled.get() || Thread.currentThread().isInterrupted();
    }

    private record MergeCheckpoint(int startIndex, byte[] currentHash) {}
}
