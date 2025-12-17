// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.common.hasher;

import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Objects;
import org.hiero.block.internal.BlockItemUnparsed;

/**
 * Provides common utility methods for hashing and combining hashes.
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

    public static final Bytes DEPTH_2_NODE_2_COMBINED;

    static {
        // For the future reserved roots, compute the combined hash of the subroot at depth 2,node 2. This hash will
        // then combine with the subroot containing the block data at the end of each round
        final Bytes combinedNullHash = combine(NULL_HASH, NULL_HASH);
        final Bytes depth3Node3 = combine(combinedNullHash, combinedNullHash);
        final Bytes depth3Node4 = combine(combinedNullHash, combinedNullHash);
        DEPTH_2_NODE_2_COMBINED = combine(depth3Node3, depth3Node4);
    }

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
            final var digest = MessageDigest.getInstance(HASH_ALGORITHM);
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
            final var digest = MessageDigest.getInstance(HASH_ALGORITHM);
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

        final var digest = sha384DigestOrThrow();
        for (int i = 0; i < itemSize; i++) {
            final BlockItemUnparsed item = blockItems.get(i);
            final BlockItemUnparsed.ItemOneOfType kind = item.item().kind();
            switch (kind) {
                case ROUND_HEADER, EVENT_HEADER ->
                    consensusHeaderHashes.put(digest.digest(
                            BlockItemUnparsed.PROTOBUF.toBytes(item).toByteArray()));
                case SIGNED_TRANSACTION ->
                    inputHashes.put(digest.digest(
                            BlockItemUnparsed.PROTOBUF.toBytes(item).toByteArray()));
                case TRANSACTION_RESULT, TRANSACTION_OUTPUT, BLOCK_HEADER ->
                    outputHashes.put(digest.digest(
                            BlockItemUnparsed.PROTOBUF.toBytes(item).toByteArray()));
                case STATE_CHANGES ->
                    stateChangesHashes.put(digest.digest(
                            BlockItemUnparsed.PROTOBUF.toBytes(item).toByteArray()));
                case TRACE_DATA ->
                    traceDataHashes.put(digest.digest(
                            BlockItemUnparsed.PROTOBUF.toBytes(item).toByteArray()));
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
        final var digest = sha384DigestOrThrow();
        ByteBuffer buffer = ByteBuffer.allocate(HASH_SIZE);
        buffer.put(digest.digest(
                BlockItemUnparsed.PROTOBUF.toBytes(blockItemUnparsed).toByteArray()));

        return buffer.flip();
    }

    /**
     * Computes the final block hash from the given block footer, timestamp and tree hashers.
     * @param blockFooter the block proof
     * @param inputTreeHasher the input tree hasher
     * @param outputTreeHasher the output tree hasher
     * @param consensusHeaderHasher the consensus header hasher
     * @param stateChangesHasher the state changes hasher
     * @param traceDataHasher the trace data hasher
     * @return the final block hash
     */
    public static Bytes computeFinalBlockHash(
            @NonNull final BlockHeader blockHeader,
            @NonNull final BlockFooter blockFooter,
            @NonNull final StreamingTreeHasher inputTreeHasher,
            @NonNull final StreamingTreeHasher outputTreeHasher,
            @NonNull final StreamingTreeHasher consensusHeaderHasher,
            @NonNull final StreamingTreeHasher stateChangesHasher,
            @NonNull final StreamingTreeHasher traceDataHasher,
            @NonNull final Bytes storedPreviousBlockHash) {
        Objects.requireNonNull(blockHeader);
        Objects.requireNonNull(blockFooter);
        Objects.requireNonNull(inputTreeHasher);
        Objects.requireNonNull(outputTreeHasher);
        Objects.requireNonNull(consensusHeaderHasher);
        Objects.requireNonNull(stateChangesHasher);
        Objects.requireNonNull(traceDataHasher);

        // only use the previous block hash from the footer if we don't have one stored already
        final Bytes previousBlockHash =
                storedPreviousBlockHash == Bytes.EMPTY ? blockFooter.previousBlockRootHash() : storedPreviousBlockHash;
        final Bytes rootOfAllPreviousBlockHashes = blockFooter.rootHashOfAllBlockHashesTree();
        final Bytes rootOfStateAtStartOfBlock = blockFooter.startOfBlockStateRootHash();
        final Bytes rootOfConsensusHeaders = consensusHeaderHasher.rootHash().join();
        final Bytes rootOfInputs = inputTreeHasher.rootHash().join();
        final Bytes rootOfOutputs = outputTreeHasher.rootHash().join();
        final Bytes rootOfStateChanges = stateChangesHasher.rootHash().join();
        final Bytes rootOfTraceData = traceDataHasher.rootHash().join();

        // Compute depth four hashes
        final var depth4Node1 = combine(previousBlockHash, rootOfAllPreviousBlockHashes);
        final var depth4Node2 = combine(rootOfStateAtStartOfBlock, rootOfConsensusHeaders);
        final var depth4Node3 = combine(rootOfInputs, rootOfOutputs);
        final var depth4Node4 = combine(rootOfStateChanges, rootOfTraceData);
        // Compute depth three hashes
        final var depth3Node1 = combine(depth4Node1, depth4Node2);
        final var depth3Node2 = combine(depth4Node3, depth4Node4);
        // Compute depth two hashes
        final var depth2Node1 = combine(depth3Node1, depth3Node2);
        // Compute depth one hash
        final Bytes depth1Node1 = combine(depth2Node1, DEPTH_2_NODE_2_COMBINED);
        // Compute the block's root hash
        final var timestamp = Timestamp.PROTOBUF.toBytes(blockHeader.blockTimestamp());
        final var depth1Node0 = noThrowSha384HashOf(timestamp);
        final var rootHash = combine(depth1Node0, depth1Node1);

        return rootHash;
    }
}
