// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.common.hasher;

import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.common.crypto.DigestType;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Objects;
import org.hiero.block.internal.BlockItemUnparsed;

/**
 * Provides common utility methods for hashing and combining hashes.
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
     * The size of an SHA-384 hash, in bytes.
     */
    public static final int HASH_SIZE = 48;
    /**
     * A constant representing a null hash, which is a zeroed-out byte array of size {@link #HASH_SIZE}.
     */
    public static final Bytes NULL_HASH = Bytes.wrap(new byte[HASH_SIZE]);

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
     * The standard name of the SHA2 384-bit hash algorithm.
     * <p>
     * This value must match what is declared for the
     * <a href="https://docs.oracle.com/en/java/javase/21/docs/specs/security/standard-names.html#messagedigest-algorithms">
     * standard message digest names</a>.
     */
    public static final String HASH_ALGORITHM = "SHA-384";

    private HashingUtilities() {
        throw new UnsupportedOperationException("Utility Class");
    }

    /**
     * Returns the SHA-384 hash of the given bytes.
     * @param bytes the bytes to hash
     * @return the SHA-384 hash of the given bytes
     */
    public static Bytes noThrowSha384HashOf(@NonNull final Bytes bytes) {
        try {
            final var digest = MessageDigest.getInstance(DigestType.SHA_384.algorithmName());
            bytes.writeTo(digest);
            return Bytes.wrap(digest.digest());
        } catch (final NoSuchAlgorithmException fatal) {
            throw new IllegalStateException(fatal);
        }
    }

    /**
     * Returns the SHA-384 hash of the given byte array.
     * @param byteArray the byte array to hash
     * @return the SHA-384 hash of the given byte array
     */
    public static byte[] noThrowSha384HashOf(@NonNull final byte[] byteArray) {
        try {
            return MessageDigest.getInstance(HASH_ALGORITHM).digest(byteArray);
        } catch (final NoSuchAlgorithmException fatal) {
            throw new IllegalStateException(fatal);
        }
    }

    /**
     * Returns a {@link MessageDigest} instance for the SHA-384 algorithm, throwing an unchecked exception if the
     * algorithm is not found.
     * @return a {@link MessageDigest} instance for the SHA-384 algorithm
     */
    public static MessageDigest sha384DigestOrThrow() {
        try {
            return MessageDigest.getInstance(HASH_ALGORITHM);
        } catch (final NoSuchAlgorithmException fatal) {
            throw new IllegalStateException(fatal);
        }
    }

    /**
     * Hashes the given left and right hashes.
     * @param leftHash the left hash
     * @param rightHash the right hash
     * @return the combined hash
     */
    public static Bytes combine(@NonNull final Bytes leftHash, @NonNull final Bytes rightHash) {
        try {
            final var digest = MessageDigest.getInstance(DigestType.SHA_384.algorithmName());
            leftHash.writeTo(digest);
            rightHash.writeTo(digest);
            return Bytes.wrap(digest.digest());
        } catch (final NoSuchAlgorithmException fatal) {
            throw new IllegalStateException(fatal);
        }
    }

    /**
     * Hashes the given left and right hashes.
     * @param leftHash the left hash
     * @param rightHash the right hash
     * @return the combined hash
     */
    public static byte[] combine(@NonNull final byte[] leftHash, @NonNull final byte[] rightHash) {
        try {
            final var digest = MessageDigest.getInstance(HASH_ALGORITHM);
            digest.update(leftHash);
            digest.update(rightHash);
            return digest.digest();
        } catch (final NoSuchAlgorithmException fatal) {
            throw new IllegalStateException(fatal);
        }
    }

    /**
     * Hash a leaf node with domain separation: {@code SHA384(0x00 || leafData)}.
     * @param leafData the serialized leaf data
     * @return the 48-byte SHA-384 hash of the prefixed leaf data
     */
    public static byte[] hashLeaf(@NonNull final byte[] leafData) {
        final MessageDigest digest = sha384DigestOrThrow();
        digest.update(LEAF_PREFIX);
        return digest.digest(leafData);
    }

    /**
     * Hash an internal node with two children using domain separation: {@code SHA384(0x02 || left || right)}.
     * @param leftHash the hash of the left child
     * @param rightHash the hash of the right child
     * @return the 48-byte SHA-384 hash of the prefixed internal node
     */
    public static byte[] hashInternalNode(@NonNull final byte[] leftHash, @NonNull final byte[] rightHash) {
        final MessageDigest digest = sha384DigestOrThrow();
        digest.update(TWO_CHILDREN_NODE_PREFIX);
        digest.update(leftHash);
        return digest.digest(rightHash);
    }

    /**
     * Hash an internal node with a single child using domain separation: {@code SHA384(0x01 || childHash)}.
     * @param childHash the hash of the single child
     * @return the 48-byte SHA-384 hash of the prefixed single-child node
     */
    public static byte[] hashInternalNodeSingleChild(@NonNull final byte[] childHash) {
        final MessageDigest digest = sha384DigestOrThrow();
        digest.update(SINGLE_CHILD_PREFIX);
        return digest.digest(childHash);
    }

    /**
     * Returns the Hashes (input and output) of a list of block items.
     * @param blockItems the block items
     * @return the Hashes of the block items
     */
    public static Hashes getBlockHashes(@NonNull List<BlockItemUnparsed> blockItems) {
        int numInputs = 0;
        int numOutputs = 0;
        int numConsensusHeaders = 0;
        int numStateChanges = 0;
        int numTraceData = 0;

        int itemSize = blockItems.size();
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

        final var inputHashes = ByteBuffer.allocate(HASH_SIZE * numInputs);
        final var outputHashes = ByteBuffer.allocate(HASH_SIZE * numOutputs);
        final var consensusHeaderHashes = ByteBuffer.allocate(HASH_SIZE * numConsensusHeaders);
        final var stateChangesHashes = ByteBuffer.allocate(HASH_SIZE * numStateChanges);
        final var traceDataHashes = ByteBuffer.allocate(HASH_SIZE * numTraceData);

        final MessageDigest digest = sha384DigestOrThrow();
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
     * returns the ByteBuffer of the hash of the given block item.
     * @param blockItemUnparsed the block item
     * @return the ByteBuffer of the hash of the given block item
     */
    public static ByteBuffer getBlockItemHash(@NonNull BlockItemUnparsed blockItemUnparsed) {
        final MessageDigest digest = sha384DigestOrThrow();
        ByteBuffer buffer = ByteBuffer.allocate(HASH_SIZE);
        digest.update(LEAF_PREFIX);
        buffer.put(digest.digest(
                BlockItemUnparsed.PROTOBUF.toBytes(blockItemUnparsed).toByteArray()));

        return buffer.flip();
    }

    /**
     * Computes the final block hash from the given block footer, timestamp and tree hashers.
     * @param blockTimestamp the block timestamp
     * @param previousBlockHash the previous block hash
     * @param rootHashOfAllPreviousBlockHashes root Hash of All previous Block Hashes
     * @param startOfBlockStateRootHash the start of block state root hash
     * @param inputTreeHasher the input tree hasher
     * @param outputTreeHasher the output tree hasher
     * @param consensusHeaderHasher the consensus header hasher
     * @param stateChangesHasher the state changes hasher
     * @param traceDataHasher the trace data hasher
     * @return the final block hash
     */
    public static Bytes computeFinalBlockHash(
            @NonNull final Timestamp blockTimestamp,
            @NonNull final Bytes previousBlockHash,
            @NonNull final Bytes rootHashOfAllPreviousBlockHashes,
            @NonNull final Bytes startOfBlockStateRootHash,
            @NonNull final StreamingTreeHasher inputTreeHasher,
            @NonNull final StreamingTreeHasher outputTreeHasher,
            @NonNull final StreamingTreeHasher consensusHeaderHasher,
            @NonNull final StreamingTreeHasher stateChangesHasher,
            @NonNull final StreamingTreeHasher traceDataHasher) {
        Objects.requireNonNull(blockTimestamp);
        Objects.requireNonNull(previousBlockHash);
        Objects.requireNonNull(rootHashOfAllPreviousBlockHashes);
        Objects.requireNonNull(startOfBlockStateRootHash);
        Objects.requireNonNull(inputTreeHasher);
        Objects.requireNonNull(outputTreeHasher);
        Objects.requireNonNull(consensusHeaderHasher);
        Objects.requireNonNull(stateChangesHasher);
        Objects.requireNonNull(traceDataHasher);

        final byte[] rootOfConsensusHeaders =
                consensusHeaderHasher.rootHash().join().toByteArray();
        final byte[] rootOfInputs = inputTreeHasher.rootHash().join().toByteArray();
        final byte[] rootOfOutputs = outputTreeHasher.rootHash().join().toByteArray();
        final byte[] rootOfStateChanges = stateChangesHasher.rootHash().join().toByteArray();
        final byte[] rootOfTraceData = traceDataHasher.rootHash().join().toByteArray();

        // Treat missing state root hash as zero hash, matching the CN convention
        final byte[] stateRootHash =
                startOfBlockStateRootHash.length() == 0 ? new byte[HASH_SIZE] : startOfBlockStateRootHash.toByteArray();

        // Depth 5: pair the 8 data leaves
        final byte[] depth5Node1 =
                hashInternalNode(previousBlockHash.toByteArray(), rootHashOfAllPreviousBlockHashes.toByteArray());
        final byte[] depth5Node2 = hashInternalNode(stateRootHash, rootOfConsensusHeaders);
        final byte[] depth5Node3 = hashInternalNode(rootOfInputs, rootOfOutputs);
        final byte[] depth5Node4 = hashInternalNode(rootOfStateChanges, rootOfTraceData);
        // Depth 4
        final byte[] depth4Node1 = hashInternalNode(depth5Node1, depth5Node2);
        final byte[] depth4Node2 = hashInternalNode(depth5Node3, depth5Node4);
        // Depth 3
        final byte[] depth3Node1 = hashInternalNode(depth4Node1, depth4Node2);
        // Depth 2: reserved subtree (single child, right side is null/reserved)
        final byte[] fixedRootTree = hashInternalNodeSingleChild(depth3Node1);
        // Root: combine timestamp leaf with fixed root tree
        final byte[] timestampLeaf =
                hashLeaf(Timestamp.PROTOBUF.toBytes(blockTimestamp).toByteArray());
        final byte[] rootHash = hashInternalNode(timestampLeaf, fixedRootTree);

        return Bytes.wrap(rootHash);
    }
}
