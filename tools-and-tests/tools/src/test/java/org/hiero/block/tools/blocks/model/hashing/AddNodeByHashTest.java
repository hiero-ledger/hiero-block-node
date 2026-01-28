// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.model.hashing;

import static org.hiero.block.tools.blocks.model.hashing.HashingUtils.hashLeaf;
import static org.hiero.block.tools.utils.Sha384.sha384Digest;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.security.MessageDigest;
import java.util.Random;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for {@link Hasher#addNodeByHash(byte[])} in both {@link StreamingHasher} and
 * {@link InMemoryTreeHasher}.
 *
 * <p>Validates that adding pre-hashed nodes produces the same root hash as adding leaves
 * whose hashes match, and that both implementations agree.
 */
@DisplayName("Hasher.addNodeByHash Tests")
class AddNodeByHashTest {

    /** Fixed seed for reproducible random data. */
    private static final long RANDOM_SEED = 0xDEADBEEF_CAFEBABEL;

    @Test
    @DisplayName("Single node by hash should produce correct root in StreamingHasher")
    void singleNodeByHashStreaming() {
        MessageDigest digest = sha384Digest();
        byte[] leafHash = hashLeaf(digest, new byte[] {1, 2, 3});

        StreamingHasher withLeaf = new StreamingHasher();
        withLeaf.addLeaf(new byte[] {1, 2, 3});

        StreamingHasher withHash = new StreamingHasher();
        withHash.addNodeByHash(leafHash);

        assertArrayEquals(withLeaf.computeRootHash(), withHash.computeRootHash());
        assertEquals(1, withHash.leafCount());
    }

    @Test
    @DisplayName("Single node by hash should produce correct root in InMemoryTreeHasher")
    void singleNodeByHashInMemory() {
        MessageDigest digest = sha384Digest();
        byte[] leafHash = hashLeaf(digest, new byte[] {1, 2, 3});

        InMemoryTreeHasher withLeaf = new InMemoryTreeHasher();
        withLeaf.addLeaf(new byte[] {1, 2, 3});

        InMemoryTreeHasher withHash = new InMemoryTreeHasher();
        withHash.addNodeByHash(leafHash);

        assertArrayEquals(withLeaf.computeRootHash(), withHash.computeRootHash());
        assertEquals(1, withHash.leafCount());
    }

    @Test
    @DisplayName("Both implementations should agree when using addNodeByHash")
    void crossValidationWithNodeByHash() {
        MessageDigest digest = sha384Digest();
        Random random = new Random(RANDOM_SEED);

        StreamingHasher streaming = new StreamingHasher();
        InMemoryTreeHasher inMemory = new InMemoryTreeHasher();

        for (int i = 0; i < 10; i++) {
            byte[] data = new byte[64];
            random.nextBytes(data);
            byte[] hash = hashLeaf(digest, data);
            streaming.addNodeByHash(hash);
            inMemory.addNodeByHash(hash);
        }

        assertArrayEquals(streaming.computeRootHash(), inMemory.computeRootHash());
        assertEquals(10, streaming.leafCount());
        assertEquals(10, inMemory.leafCount());
    }

    @ParameterizedTest(name = "{0} nodes")
    @ValueSource(ints = {1, 2, 3, 4, 5, 7, 8, 15, 16, 31, 32, 100})
    @DisplayName("addNodeByHash should match addLeaf with pre-hashed data for various counts")
    void nodeByHashMatchesLeafForVariousCounts(int count) {
        MessageDigest digest = sha384Digest();
        Random random = new Random(RANDOM_SEED);

        StreamingHasher leafHasher = new StreamingHasher();
        StreamingHasher nodeHasher = new StreamingHasher();
        InMemoryTreeHasher leafHasherInMem = new InMemoryTreeHasher();
        InMemoryTreeHasher nodeHasherInMem = new InMemoryTreeHasher();

        for (int i = 0; i < count; i++) {
            byte[] data = new byte[32];
            random.nextBytes(data);
            byte[] hash = hashLeaf(digest, data);

            leafHasher.addLeaf(data);
            nodeHasher.addNodeByHash(hash);
            leafHasherInMem.addLeaf(data);
            nodeHasherInMem.addNodeByHash(hash);
        }

        assertArrayEquals(
                leafHasher.computeRootHash(),
                nodeHasher.computeRootHash(),
                "StreamingHasher: addNodeByHash should match addLeaf");
        assertArrayEquals(
                leafHasherInMem.computeRootHash(),
                nodeHasherInMem.computeRootHash(),
                "InMemoryTreeHasher: addNodeByHash should match addLeaf");
        assertArrayEquals(
                nodeHasher.computeRootHash(),
                nodeHasherInMem.computeRootHash(),
                "Both implementations should agree for addNodeByHash");
    }

    @Test
    @DisplayName("Mixing addLeaf and addNodeByHash should work correctly")
    void mixedAddLeafAndAddNodeByHash() {
        MessageDigest digest = sha384Digest();

        byte[] data0 = {10, 20, 30};
        byte[] data1 = {40, 50, 60};
        byte[] data2 = {70, 80, 90};
        byte[] hash1 = hashLeaf(digest, data1);

        StreamingHasher expected = new StreamingHasher();
        expected.addLeaf(data0);
        expected.addLeaf(data1);
        expected.addLeaf(data2);

        StreamingHasher mixed = new StreamingHasher();
        mixed.addLeaf(data0);
        mixed.addNodeByHash(hash1);
        mixed.addLeaf(data2);

        assertArrayEquals(expected.computeRootHash(), mixed.computeRootHash());
        assertEquals(3, mixed.leafCount());

        // Same test for InMemoryTreeHasher
        InMemoryTreeHasher expectedInMem = new InMemoryTreeHasher();
        expectedInMem.addLeaf(data0);
        expectedInMem.addLeaf(data1);
        expectedInMem.addLeaf(data2);

        InMemoryTreeHasher mixedInMem = new InMemoryTreeHasher();
        mixedInMem.addLeaf(data0);
        mixedInMem.addNodeByHash(hash1);
        mixedInMem.addLeaf(data2);

        assertArrayEquals(expectedInMem.computeRootHash(), mixedInMem.computeRootHash());
        assertEquals(3, mixedInMem.leafCount());
    }

    @Test
    @DisplayName("Single leaf tree root hash is exactly hash(0x00 || data) with no internal node wrapping")
    void singleLeafTreeRootIsLeafHashOnly() {
        MessageDigest digest = sha384Digest();
        byte[] data = {1, 2, 3, 4, 5};
        byte[] expectedLeafHash = hashLeaf(digest, data);

        // StreamingHasher: root should be exactly the leaf hash, no 0x01 internal node
        StreamingHasher streaming = new StreamingHasher();
        streaming.addLeaf(data);
        assertArrayEquals(
                expectedLeafHash,
                streaming.computeRootHash(),
                "StreamingHasher single-leaf root should be hash(0x00 || data) with no internal node wrapping");

        // InMemoryTreeHasher: same expectation
        InMemoryTreeHasher inMemory = new InMemoryTreeHasher();
        inMemory.addLeaf(data);
        assertArrayEquals(
                expectedLeafHash,
                inMemory.computeRootHash(),
                "InMemoryTreeHasher single-leaf root should be hash(0x00 || data) with no internal node wrapping");
    }

    @Test
    @DisplayName("addNodeByHash with subtree root hashes produces different root than addLeaf")
    void nodeByHashWithArbitraryHashDiffersFromLeaf() {
        // When we pass an arbitrary hash (not from hashLeaf), the result should differ
        // from addLeaf with raw data, since addLeaf applies the leaf prefix
        byte[] arbitraryHash = new byte[48];
        new Random(42).nextBytes(arbitraryHash);

        StreamingHasher byHash = new StreamingHasher();
        byHash.addNodeByHash(arbitraryHash);

        // The root should just be the hash itself (single node)
        assertArrayEquals(arbitraryHash, byHash.computeRootHash());
    }
}
