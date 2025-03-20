// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.common.hasher;

import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Objects;
import org.hiero.hapi.block.node.BlockItemUnparsed;

/**
 * Provides common utility methods for hashing and combining hashes.
 */
public final class HashingUtilities {
    private HashingUtilities() {
        throw new UnsupportedOperationException("Utility Class");
    }

    /**
     * The size of a SHA-384 hash, in bytes.
     */
    public static final int HASH_SIZE = 48;

    /**
     * The standard name of the SHA2 384-bit hash algorithm.
     *
     * This value must match what is declared for the
     * <a href="https://docs.oracle.com/en/java/javase/21/docs/specs/security/standard-names.html#messagedigest-algorithms">
     * standard message digest names</a>.
     */
    public static final String HASH_ALGORITHM = "SHA-384";

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
     * Returns the Hashes (input and output) of a list of block items.
     * @param blockItems the block items
     * @return the Hashes of the block items
     */
    public static Hashes getBlockHashes(@NonNull List<BlockItemUnparsed> blockItems) {
        int numInputs = 0;
        int numOutputs = 0;
        int itemSize = blockItems.size();
        for (int i = 0; i < itemSize; i++) {
            final BlockItemUnparsed item = blockItems.get(i);
            final BlockItemUnparsed.ItemOneOfType kind = item.item().kind();
            switch (kind) {
                case EVENT_HEADER, EVENT_TRANSACTION, ROUND_HEADER -> numInputs++;
                case TRANSACTION_RESULT, TRANSACTION_OUTPUT, STATE_CHANGES, BLOCK_HEADER -> numOutputs++;
            }
        }

        final var inputHashes = ByteBuffer.allocate(HASH_SIZE * numInputs);
        final var outputHashes = ByteBuffer.allocate(HASH_SIZE * numOutputs);
        final var digest = sha384DigestOrThrow();
        for (int i = 0; i < itemSize; i++) {
            final BlockItemUnparsed item = blockItems.get(i);
            final BlockItemUnparsed.ItemOneOfType kind = item.item().kind();
            switch (kind) {
                case EVENT_HEADER, EVENT_TRANSACTION, ROUND_HEADER -> inputHashes.put(
                        digest.digest(BlockItemUnparsed.PROTOBUF.toBytes(item).toByteArray()));
                case TRANSACTION_RESULT, TRANSACTION_OUTPUT, STATE_CHANGES, BLOCK_HEADER -> outputHashes.put(
                        digest.digest(BlockItemUnparsed.PROTOBUF.toBytes(item).toByteArray()));
            }
        }

        return new Hashes(inputHashes.flip(), outputHashes.flip());
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
     * Computes the final block hash from the given block proof and tree hashers.
     * @param blockProof the block proof
     * @param inputTreeHasher the input tree hasher
     * @param outputTreeHasher the output tree hasher
     * @return the final block hash
     */
    public static Bytes computeFinalBlockHash(
            @NonNull final BlockProof blockProof,
            @NonNull final StreamingTreeHasher inputTreeHasher,
            @NonNull final StreamingTreeHasher outputTreeHasher) {
        Objects.requireNonNull(blockProof);
        Objects.requireNonNull(inputTreeHasher);
        Objects.requireNonNull(outputTreeHasher);

        Bytes inputHash = inputTreeHasher.rootHash().join();
        Bytes outputHash = outputTreeHasher.rootHash().join();
        Bytes providedLasBlockHash = blockProof.previousBlockRootHash();
        Bytes providedBlockStartStateHash = blockProof.startOfBlockStateRootHash();

        final var leftParent = combine(providedLasBlockHash, inputHash);
        final var rightParent = combine(outputHash, providedBlockStartStateHash);
        return combine(leftParent, rightParent);
    }
}
