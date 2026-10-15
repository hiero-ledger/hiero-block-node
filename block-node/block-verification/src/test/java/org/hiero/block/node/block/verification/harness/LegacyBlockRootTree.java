// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.harness;

import static org.hiero.block.common.hasher.HashingUtilities.LEAF_PREFIX;
import static org.hiero.block.common.hasher.HashingUtilities.TWO_CHILDREN_NODE_PREFIX;

import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import org.hiero.block.common.hasher.HashAlgorithm;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;

/// Computes the HIP-1424 block root tree of a harness block with any [HashAlgorithm].
///
/// The common hashing utilities and hashers compute with SHA2_256 only, while a release from
/// before the move to SHA-256 verifies blocks hashed with SHA2_384. The harness hashes a chain of
/// an algorithm other than SHA2_256 with this class. It builds the same tree as the production
/// `BlockHasher`: every item leaf is `hash(0x00 || itemBytes)` and is routed into one of the five
/// item category subtrees, every subtree is folded by the streaming fold, and the fixed 16 leaf
/// Merkle Mountain Top holds the previous block hash, the root of all previous block hashes, the
/// start of block state root, the five subtree roots and eight empty extension subtrees. The block
/// root is `hash(0x02 || timestampLeaf || mountainTopRoot)`.
final class LegacyBlockRootTree {
    /// The number of extension subtrees of the Merkle Mountain Top, all empty in a harness block.
    private static final int EXTENSION_SUBTREES = 8;

    private LegacyBlockRootTree() {}

    /// Computes the block root hash of a harness block. A harness block carries only a header,
    /// signed transactions, a round header, a footer and a proof, so the items a harness block
    /// never carries are refused instead of being placed into a subtree.
    ///
    /// @param algorithm the algorithm every hash of the tree is computed with
    /// @param block the harness block, its footer hashes having the digest size of the algorithm
    /// @return the block root hash
    static Bytes blockRootHash(final HashAlgorithm algorithm, final TestBlock block) throws NoSuchAlgorithmException {
        final List<byte[]> consensusHeaderLeaves = new ArrayList<>();
        final List<byte[]> inputLeaves = new ArrayList<>();
        final List<byte[]> outputLeaves = new ArrayList<>();
        final List<byte[]> stateChangesLeaves = new ArrayList<>();
        final List<byte[]> traceDataLeaves = new ArrayList<>();
        for (final BlockItemUnparsed item : block.blockUnparsed().blockItems()) {
            final List<byte[]> subtreeLeaves =
                    switch (item.item().kind()) {
                        case ROUND_HEADER, EVENT_HEADER -> consensusHeaderLeaves;
                        case SIGNED_TRANSACTION -> inputLeaves;
                        case BLOCK_HEADER, TRANSACTION_RESULT, TRANSACTION_OUTPUT, RECORD_FILE -> outputLeaves;
                        case STATE_CHANGES -> stateChangesLeaves;
                        case TRACE_DATA -> traceDataLeaves;
                        case BLOCK_FOOTER, BLOCK_PROOF -> null;
                        case UNSET, FILTERED_SINGLE_ITEM, REDACTED_ITEM ->
                            throw new IllegalArgumentException("A harness block carries no %s item"
                                    .formatted(item.item().kind()));
                    };
            if (subtreeLeaves != null) {
                subtreeLeaves.add(digest(
                        algorithm,
                        LEAF_PREFIX,
                        BlockItemUnparsed.PROTOBUF.toBytes(item).toByteArray()));
            }
        }
        final BlockFooter footer = block.footer();
        final Bytes stateRootHash = footer.startOfBlockStateRootHash();
        final List<byte[]> mountainTopLeaves = new ArrayList<>();
        mountainTopLeaves.add(footer.previousBlockRootHash().toByteArray());
        mountainTopLeaves.add(footer.rootHashOfAllBlockHashesTree().toByteArray());
        mountainTopLeaves.add((stateRootHash.length() == 0 ? algorithm.emptyTreeHash() : stateRootHash).toByteArray());
        mountainTopLeaves.add(treeRoot(algorithm, consensusHeaderLeaves));
        mountainTopLeaves.add(treeRoot(algorithm, inputLeaves));
        mountainTopLeaves.add(treeRoot(algorithm, outputLeaves));
        mountainTopLeaves.add(treeRoot(algorithm, stateChangesLeaves));
        mountainTopLeaves.add(treeRoot(algorithm, traceDataLeaves));
        for (int extension = 0; extension < EXTENSION_SUBTREES; extension++) {
            mountainTopLeaves.add(algorithm.emptyTreeHash().toByteArray());
        }
        final byte[] timestampLeaf = digest(
                algorithm,
                LEAF_PREFIX,
                Timestamp.PROTOBUF.toBytes(block.header().blockTimestamp()).toByteArray());
        return Bytes.wrap(
                digest(algorithm, TWO_CHILDREN_NODE_PREFIX, timestampLeaf, treeRoot(algorithm, mountainTopLeaves)));
    }

    /// Folds the given leaf hashes the way the streaming hashers do: two sibling subtrees of the
    /// same height are combined into `hash(0x02 || left || right)` as the leaves arrive, the open
    /// branches left at the end are folded right to left, and an empty tree has the root
    /// [HashAlgorithm#emptyTreeHash()]. Block root hashes enter the tree of all previous block
    /// hashes the same way, as nodes that are not hashed again.
    ///
    /// @param algorithm the algorithm every node of the tree is computed with
    /// @param leafHashes the leaf hashes in order, each one digest of the algorithm
    /// @return the root of the tree
    static byte[] treeRoot(final HashAlgorithm algorithm, final List<byte[]> leafHashes)
            throws NoSuchAlgorithmException {
        final byte[] result;
        if (leafHashes.isEmpty()) {
            result = algorithm.emptyTreeHash().toByteArray();
        } else {
            final List<byte[]> pending = new ArrayList<>();
            for (int index = 0; index < leafHashes.size(); index++) {
                pending.add(leafHashes.get(index));
                for (long position = index; (position & 1L) == 1; position >>= 1) {
                    final byte[] right = pending.removeLast();
                    final byte[] left = pending.removeLast();
                    pending.add(digest(algorithm, TWO_CHILDREN_NODE_PREFIX, left, right));
                }
            }
            byte[] root = pending.getLast();
            for (int index = pending.size() - 2; index >= 0; index--) {
                root = digest(algorithm, TWO_CHILDREN_NODE_PREFIX, pending.get(index), root);
            }
            result = root;
        }
        return result;
    }

    /// Returns the digest of the given algorithm over the concatenation of the given parts.
    private static byte[] digest(final HashAlgorithm algorithm, final byte[]... parts) throws NoSuchAlgorithmException {
        final MessageDigest digest = algorithm.newDigest();
        for (final byte[] part : parts) {
            digest.update(part);
        }
        return digest.digest();
    }
}
