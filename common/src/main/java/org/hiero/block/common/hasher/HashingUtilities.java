// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.common.hasher;

import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import org.hiero.block.internal.BlockItemUnparsed;

/**
 * Provides common utility methods for hashing and combining hashes.
 * <p>
 * Every block hashing method computes with {@link HashAlgorithm#SHA2_256}, the algorithm of the
 * block root tree. The single exception is {@link #computeV6SignedPayload(Bytes)}, the record file
 * signature payload of a wrapped record block: it is SHA-384 by definition of the record file
 * format, independent of the algorithm the block root tree is computed with.
 * <p>
 * Domain-separated Merkle tree hashing uses single-byte prefixes to ensure leaf hashes
 * and internal node hashes occupy distinct hash spaces:
 * <ul>
 *   <li>{@code 0x00} - Leaf node: {@code hash(0x00 || leafData)}</li>
 *   <li>{@code 0x01} - Single-child internal node: {@code hash(0x01 || childHash)}</li>
 *   <li>{@code 0x02} - Two-child internal node: {@code hash(0x02 || leftHash || rightHash)}</li>
 * </ul>
 */
public final class HashingUtilities {

    /**
     * Prefix byte for leaf node hashes: {@code hash(0x00 || leafData)}.
     */
    public static final byte[] LEAF_PREFIX = new byte[] {0x00};

    /**
     * Prefix byte for single-child internal node hashes: {@code hash(0x01 || childHash)}.
     */
    public static final byte[] SINGLE_CHILD_PREFIX = new byte[] {0x01};

    /**
     * Prefix byte for two-child internal node hashes: {@code hash(0x02 || leftHash || rightHash)}.
     */
    public static final byte[] TWO_CHILDREN_NODE_PREFIX = new byte[] {0x02};

    /**
     * The algorithm every block hashing method of this class computes with.
     */
    private static final HashAlgorithm ALGORITHM = HashAlgorithm.SHA2_256;

    private HashingUtilities() {
        throw new UnsupportedOperationException("Utility Class");
    }

    /**
     * Computes the version 6 record-file signed payload: {@code SHA-384(int32be(6) || recordFileContents)}.
     * <p>
     * This is the exact payload the consensus node signs (and the block node verifies) for a version 6
     * record-file (WRB) block proof. The {@code int32be(6)} prefix is the four bytes
     * {@code 0x00 0x00 0x00 0x06}. Keeping this in one place helps to ensure the test signer and the verifier
     * agree byte-for-byte. The record file format defines this payload as SHA-384, so the algorithm is fixed
     * here and independent of the algorithm the block root tree is computed with.
     *
     * @param recordFileContents the verbatim {@code record_file_contents} bytes
     * @return the 48-byte SHA-384 digest
     * @throws NoSuchAlgorithmException if the Java runtime provides no implementation of the algorithm
     */
    public static byte[] computeV6SignedPayload(final Bytes recordFileContents) throws NoSuchAlgorithmException {
        final MessageDigest digest = HashAlgorithm.SHA2_384.newDigest();
        digest.update(new byte[] {0, 0, 0, 6}); // int32(6) big-endian
        recordFileContents.writeTo(digest);
        return digest.digest();
    }

    /**
     * Hash a leaf node with domain separation: {@code hash(0x00 || leafData)}.
     * @param leafData the serialized leaf data
     * @return the hash of the prefixed leaf data, one SHA-256 digest
     * @throws NoSuchAlgorithmException if the Java runtime provides no implementation of the algorithm
     */
    public static byte[] hashLeaf(final byte[] leafData) throws NoSuchAlgorithmException {
        final MessageDigest digest = ALGORITHM.newDigest();
        digest.update(LEAF_PREFIX);
        return digest.digest(leafData);
    }

    /**
     * Hash an internal node with two children using domain separation: {@code hash(0x02 || left || right)}.
     * @param leftHash the hash of the left child
     * @param rightHash the hash of the right child
     * @return the hash of the prefixed internal node, one SHA-256 digest
     * @throws NoSuchAlgorithmException if the Java runtime provides no implementation of the algorithm
     */
    public static byte[] hashInternalNode(final byte[] leftHash, final byte[] rightHash)
            throws NoSuchAlgorithmException {
        final MessageDigest digest = ALGORITHM.newDigest();
        digest.update(TWO_CHILDREN_NODE_PREFIX);
        digest.update(leftHash);
        return digest.digest(rightHash);
    }

    /**
     * Hash an internal node with a single child using domain separation: {@code hash(0x01 || childHash)}.
     * @param childHash the hash of the single child
     * @return the hash of the prefixed single-child node, one SHA-256 digest
     * @throws NoSuchAlgorithmException if the Java runtime provides no implementation of the algorithm
     */
    public static byte[] hashInternalNodeSingleChild(final byte[] childHash) throws NoSuchAlgorithmException {
        final MessageDigest digest = ALGORITHM.newDigest();
        digest.update(SINGLE_CHILD_PREFIX);
        return digest.digest(childHash);
    }

    /**
     * Returns the Hashes (input and output) of a list of block items. Each buffer holds the leaf
     * hashes of one item category back to back, every leaf hash being one SHA-256 digest.
     * @param blockItems the block items
     * @return the Hashes of the block items
     * @throws NoSuchAlgorithmException if the Java runtime provides no implementation of the algorithm
     */
    public static Hashes getBlockHashes(final List<BlockItemUnparsed> blockItems) throws NoSuchAlgorithmException {
        int numInputs = 0;
        int numOutputs = 0;
        int numConsensusHeaders = 0;
        int numStateChanges = 0;
        int numTraceData = 0;

        final int itemSize = blockItems.size();
        for (int i = 0; i < itemSize; i++) {
            final BlockItemUnparsed item = blockItems.get(i);
            final BlockItemUnparsed.ItemOneOfType kind = item.item().kind();
            switch (kind) {
                case ROUND_HEADER, EVENT_HEADER -> numConsensusHeaders++;
                case SIGNED_TRANSACTION -> numInputs++;
                case TRANSACTION_RESULT, TRANSACTION_OUTPUT, BLOCK_HEADER -> numOutputs++;
                case STATE_CHANGES -> numStateChanges++;
                case TRACE_DATA -> numTraceData++;
            }
        }

        final int hashSize = ALGORITHM.hashSize();
        final ByteBuffer inputHashes = ByteBuffer.allocate(hashSize * numInputs);
        final ByteBuffer outputHashes = ByteBuffer.allocate(hashSize * numOutputs);
        final ByteBuffer consensusHeaderHashes = ByteBuffer.allocate(hashSize * numConsensusHeaders);
        final ByteBuffer stateChangesHashes = ByteBuffer.allocate(hashSize * numStateChanges);
        final ByteBuffer traceDataHashes = ByteBuffer.allocate(hashSize * numTraceData);

        final MessageDigest digest = ALGORITHM.newDigest();
        for (int i = 0; i < itemSize; i++) {
            final BlockItemUnparsed item = blockItems.get(i);
            final BlockItemUnparsed.ItemOneOfType kind = item.item().kind();
            switch (kind) {
                case ROUND_HEADER, EVENT_HEADER -> {
                    // Incrementally feed prefix then item bytes into a single hash computation.
                    // This avoids concatenating byte arrays while producing the same digest.
                    digest.update(LEAF_PREFIX);
                    consensusHeaderHashes.put(digest.digest(
                            BlockItemUnparsed.PROTOBUF.toBytes(item).toByteArray()));
                }
                case SIGNED_TRANSACTION -> {
                    digest.update(LEAF_PREFIX);
                    inputHashes.put(digest.digest(
                            BlockItemUnparsed.PROTOBUF.toBytes(item).toByteArray()));
                }
                case TRANSACTION_RESULT, TRANSACTION_OUTPUT, BLOCK_HEADER -> {
                    digest.update(LEAF_PREFIX);
                    outputHashes.put(digest.digest(
                            BlockItemUnparsed.PROTOBUF.toBytes(item).toByteArray()));
                }
                case STATE_CHANGES -> {
                    digest.update(LEAF_PREFIX);
                    stateChangesHashes.put(digest.digest(
                            BlockItemUnparsed.PROTOBUF.toBytes(item).toByteArray()));
                }
                case TRACE_DATA -> {
                    digest.update(LEAF_PREFIX);
                    traceDataHashes.put(digest.digest(
                            BlockItemUnparsed.PROTOBUF.toBytes(item).toByteArray()));
                }
            }
        }

        return new Hashes(
                inputHashes.flip(),
                outputHashes.flip(),
                consensusHeaderHashes.flip(),
                stateChangesHashes.flip(),
                traceDataHashes.flip());
    }

    /**
     * Returns the ByteBuffer of the leaf hash of the given block item: {@code hash(0x00 || itemBytes)}.
     * @param blockItemUnparsed the block item
     * @return the ByteBuffer of the hash of the given block item, one SHA-256 digest
     * @throws NoSuchAlgorithmException if the Java runtime provides no implementation of the algorithm
     */
    public static ByteBuffer getBlockItemHash(final BlockItemUnparsed blockItemUnparsed)
            throws NoSuchAlgorithmException {
        final MessageDigest digest = ALGORITHM.newDigest();
        final ByteBuffer buffer = ByteBuffer.allocate(ALGORITHM.hashSize());
        digest.update(LEAF_PREFIX);
        buffer.put(digest.digest(
                BlockItemUnparsed.PROTOBUF.toBytes(blockItemUnparsed).toByteArray()));
        return buffer.flip();
    }

    /**
     * Computes the final block hash from the given block footer, timestamp and tree hashers.
     * <p>
     * The block root tree is the fixed 16-leaf "Merkle Mountain Top" defined by HIP-1424.
     * Leaves 1 to 8 are the previous block hash, the root of all previous block hashes, the
     * start of block state root and the five block item category subtrees. Leaves 9 to 16 are
     * the extension subtrees, reserved for future block item categories. Every leaf is fed to
     * the hasher on every call; an empty subtree hasher contributes
     * {@link HashAlgorithm#emptyTreeHash()} via its own {@code rootHash()}. Callers pass a fresh
     * (empty) hasher for any extension slot that carries no data in this block.
     * <p>
     * Every leaf of the tree is one SHA-256 digest: the previous block hash, the root of all
     * previous block hashes and a non-empty start of block state root must have exactly that size.
     * A value of another size cannot be hashed into a meaningful root, so it is refused instead of
     * being silently truncated or padded.
     * @param blockTimestamp the block timestamp
     * @param previousBlockHash the previous block hash
     * @param rootHashOfAllPreviousBlockHashes root Hash of All previous Block Hashes
     * @param startOfBlockStateRootHash the start of block state root hash, empty when the block has none
     * @param inputTreeHasher the input tree hasher
     * @param outputTreeHasher the output tree hasher
     * @param consensusHeaderHasher the consensus header hasher
     * @param stateChangesHasher the state changes hasher
     * @param traceDataHasher the trace data hasher
     * @param extensionSubtreeHasherZero the Extension 0 subtree hasher at leaf position 9
     * @param extensionSubtreeHasherOne the Extension 1 subtree hasher at leaf position 10
     * @param extensionSubtreeHasherTwo the Extension 2 subtree hasher at leaf position 11
     * @param extensionSubtreeHasherThree the Extension 3 subtree hasher at leaf position 12
     * @param extensionSubtreeHasherFour the Extension 4 subtree hasher at leaf position 13
     * @param extensionSubtreeHasherFive the Extension 5 subtree hasher at leaf position 14
     * @param extensionSubtreeHasherSix the Extension 6 subtree hasher at leaf position 15
     * @param extensionSubtreeHasherSeven the Extension 7 subtree hasher at leaf position 16
     * @return the final block hash
     * @throws IllegalArgumentException if a fixed position hash does not have the SHA-256 digest size
     * @throws NoSuchAlgorithmException if the Java runtime provides no implementation of the algorithm
     */
    public static Bytes computeFinalBlockHash( // @todo(3372) re-evaluate this signature
            final Timestamp blockTimestamp,
            final Bytes previousBlockHash,
            final Bytes rootHashOfAllPreviousBlockHashes,
            final Bytes startOfBlockStateRootHash,
            final StreamingTreeHasher inputTreeHasher,
            final StreamingTreeHasher outputTreeHasher,
            final StreamingTreeHasher consensusHeaderHasher,
            final StreamingTreeHasher stateChangesHasher,
            final StreamingTreeHasher traceDataHasher,
            final StreamingTreeHasher extensionSubtreeHasherZero,
            final StreamingTreeHasher extensionSubtreeHasherOne,
            final StreamingTreeHasher extensionSubtreeHasherTwo,
            final StreamingTreeHasher extensionSubtreeHasherThree,
            final StreamingTreeHasher extensionSubtreeHasherFour,
            final StreamingTreeHasher extensionSubtreeHasherFive,
            final StreamingTreeHasher extensionSubtreeHasherSix,
            final StreamingTreeHasher extensionSubtreeHasherSeven)
            throws NoSuchAlgorithmException {
        // Merkle Mountain Top: feed all 16 leaves (positions 0-7 pre-defined, 8-15 extensions)
        // into a single NaiveStreamingTreeHasher, the same streaming hasher used within each
        // subtree. Empty subtree hashers naturally contribute the empty tree hash via their own
        // rootHash(). This fixes the tree shape across all presence patterns so Merkle proof
        // paths for any fixed position are independent of which other positions are populated.
        // See issue #3377.
        final int hashSize = ALGORITHM.hashSize();
        final byte[] previousBlockHashBytes =
                requireHashSize(previousBlockHash.toByteArray(), hashSize, "previousBlockHash");
        final byte[] rootHashOfAllPreviousBlockHashesBytes = requireHashSize(
                rootHashOfAllPreviousBlockHashes.toByteArray(), hashSize, "rootHashOfAllPreviousBlockHashes");
        final byte[] stateRootHash;
        if (startOfBlockStateRootHash.length() == 0) {
            stateRootHash = ALGORITHM.emptyTreeHash().toByteArray();
        } else {
            stateRootHash =
                    requireHashSize(startOfBlockStateRootHash.toByteArray(), hashSize, "startOfBlockStateRootHash");
        }
        final NaiveStreamingTreeHasher mountainTopHasher = new NaiveStreamingTreeHasher();
        mountainTopHasher.addLeaf(ByteBuffer.wrap(previousBlockHashBytes));
        mountainTopHasher.addLeaf(ByteBuffer.wrap(rootHashOfAllPreviousBlockHashesBytes));
        mountainTopHasher.addLeaf(ByteBuffer.wrap(stateRootHash));
        mountainTopHasher.addLeaf(subtreeRootBuffer(consensusHeaderHasher));
        mountainTopHasher.addLeaf(subtreeRootBuffer(inputTreeHasher));
        mountainTopHasher.addLeaf(subtreeRootBuffer(outputTreeHasher));
        mountainTopHasher.addLeaf(subtreeRootBuffer(stateChangesHasher));
        mountainTopHasher.addLeaf(subtreeRootBuffer(traceDataHasher));
        mountainTopHasher.addLeaf(subtreeRootBuffer(extensionSubtreeHasherZero));
        mountainTopHasher.addLeaf(subtreeRootBuffer(extensionSubtreeHasherOne));
        mountainTopHasher.addLeaf(subtreeRootBuffer(extensionSubtreeHasherTwo));
        mountainTopHasher.addLeaf(subtreeRootBuffer(extensionSubtreeHasherThree));
        mountainTopHasher.addLeaf(subtreeRootBuffer(extensionSubtreeHasherFour));
        mountainTopHasher.addLeaf(subtreeRootBuffer(extensionSubtreeHasherFive));
        mountainTopHasher.addLeaf(subtreeRootBuffer(extensionSubtreeHasherSix));
        mountainTopHasher.addLeaf(subtreeRootBuffer(extensionSubtreeHasherSeven));
        final byte[] mountainTopRoot = mountainTopHasher.rootHash().toByteArray();
        final byte[] timestampLeaf =
                hashLeaf(Timestamp.PROTOBUF.toBytes(blockTimestamp).toByteArray());
        final byte[] rootHash = hashInternalNode(timestampLeaf, mountainTopRoot);

        return Bytes.wrap(rootHash);
    }

    /**
     * Materializes a subtree hasher's root as a {@link ByteBuffer} ready to feed into the
     * Mountain Top hasher.
     * @throws NoSuchAlgorithmException if the Java runtime provides no implementation of the algorithm
     */
    private static ByteBuffer subtreeRootBuffer(final StreamingTreeHasher hasher) throws NoSuchAlgorithmException {
        return ByteBuffer.wrap(hasher.rootHash().toByteArray());
    }

    /**
     * Validates that a fixed position Mountain Top value has exactly the digest size of the
     * algorithm. The streaming hasher reads exactly one digest per leaf, so a longer value would
     * be silently truncated and a shorter one refused by the hasher with a less specific message;
     * either way the root would not be the root of the block, so the value is refused up front.
     * @param hash the hash bytes to validate
     * @param hashSize the digest size of the algorithm
     * @param name the leaf name used in the error message
     * @return the input hash, unchanged, if it has exactly {@code hashSize} bytes
     * @throws IllegalArgumentException if the input does not have exactly {@code hashSize} bytes
     */
    private static byte[] requireHashSize(final byte[] hash, final int hashSize, final String name) {
        final byte[] result;
        if (hash.length != hashSize) {
            throw new IllegalArgumentException(
                    "%s must be exactly %d bytes, got %d".formatted(name, hashSize, hash.length));
        } else {
            result = hash;
        }
        return result;
    }
}
