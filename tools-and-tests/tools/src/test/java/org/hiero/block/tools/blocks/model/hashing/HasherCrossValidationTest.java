// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.model.hashing;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Cross-validation tests that verify {@link StreamingHasher} and {@link InMemoryTreeHasher} produce identical root
 * hashes for various tree sizes and configurations.
 *
 * <p>This test class provides comprehensive validation across multiple dimensions:
 * <ul>
 *   <li><b>Tree sizes:</b> Power-of-2, odd, prime, even non-power-of-2, Fibonacci, boundary cases</li>
 *   <li><b>Data patterns:</b> Random data, identical leaves, empty leaves, large leaves, variable sizes</li>
 *   <li><b>Incremental verification:</b> Root hash comparison after each leaf addition</li>
 *   <li><b>Stress testing:</b> Large trees up to 4096 leaves</li>
 * </ul>
 *
 * <p>All tests use seeded random data to ensure reproducibility. The fixed seed {@code 0xDEADBEEF_CAFEBABE}
 * generates consistent test data across runs.
 *
 * <p>These tests are critical for ensuring that the memory-efficient {@link StreamingHasher} and the
 * full-tree {@link InMemoryTreeHasher} produce cryptographically identical results.
 */
@DisplayName("Hasher Cross-Validation Tests")
class HasherCrossValidationTest {

    /** Fixed seed for reproducible random data generation. */
    private static final long RANDOM_SEED = 0xDEADBEEF_CAFEBABEL;

    /** Maximum leaf data size in bytes for random tests. */
    private static final int MAX_LEAF_SIZE = 4096;

    /** Minimum leaf data size in bytes for random tests. */
    private static final int MIN_LEAF_SIZE = 1;

    // ========== Power of 2 Leaf Count Tests ==========

    /**
     * Provides power-of-2 leaf counts from 1 to 1024.
     *
     * <p>Power-of-2 trees form perfect binary trees where every level is completely filled.
     * These are the simplest case for Merkle tree construction.
     */
    @ParameterizedTest(name = "Power of 2: {0} leaves")
    @ValueSource(ints = {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024})
    @DisplayName("Power-of-2 leaf counts should produce matching root hashes")
    void testPowerOfTwoLeafCounts(int numLeaves) {
        assertHashersMatch(numLeaves, RANDOM_SEED);
    }

    // ========== Odd Leaf Count Tests ==========

    /**
     * Tests odd leaf counts which require special handling in the tree construction.
     *
     * <p>Odd counts create trees where the rightmost subtree has one less leaf than the left,
     * requiring the right-to-left folding algorithm to produce the correct root.
     */
    @ParameterizedTest(name = "Odd count: {0} leaves")
    @ValueSource(ints = {1, 3, 5, 7, 9, 11, 15, 17, 31, 33, 63, 65, 127, 129, 255, 257, 511, 513, 1023, 1025})
    @DisplayName("Odd leaf counts should produce matching root hashes")
    void testOddLeafCounts(int numLeaves) {
        assertHashersMatch(numLeaves, RANDOM_SEED);
    }

    // ========== Prime Leaf Count Tests ==========

    /**
     * Tests all prime numbers up to 1021.
     *
     * <p>Prime numbers cannot be factored, creating maximally unbalanced subtrees at various levels.
     * This stress-tests the tree construction algorithm.
     */
    @ParameterizedTest(name = "Prime count: {0} leaves")
    @ValueSource(
            ints = {
                2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97, 101,
                103, 107, 109, 113, 127, 131, 137, 139, 149, 151, 157, 163, 167, 173, 179, 181, 191, 193, 197, 199, 211,
                223, 227, 229, 233, 239, 241, 251, 257, 263, 269, 271, 277, 281, 283, 293, 307, 311, 313, 317, 331, 337,
                347, 349, 353, 359, 367, 373, 379, 383, 389, 397, 401, 409, 419, 421, 431, 433, 439, 443, 449, 457, 461,
                463, 467, 479, 487, 491, 499, 503, 509, 521, 523, 541, 547, 557, 563, 569, 571, 577, 587, 593, 599, 601,
                607, 613, 617, 619, 631, 641, 643, 647, 653, 659, 661, 673, 677, 683, 691, 701, 709, 719, 727, 733, 739,
                743, 751, 757, 761, 769, 773, 787, 797, 809, 811, 821, 823, 827, 829, 839, 853, 857, 859, 863, 877, 881,
                883, 887, 907, 911, 919, 929, 937, 941, 947, 953, 967, 971, 977, 983, 991, 997, 1009, 1013, 1019, 1021
            })
    @DisplayName("Prime leaf counts should produce matching root hashes")
    void testPrimeLeafCounts(int numLeaves) {
        assertHashersMatch(numLeaves, RANDOM_SEED);
    }

    // ========== Even Non-Power-of-2 Leaf Count Tests ==========

    /**
     * Tests even numbers that are not powers of 2.
     *
     * <p>These create trees with mixed subtree sizes, testing the algorithm's ability to handle
     * intermediate complexity between power-of-2 (simple) and prime (complex) cases.
     */
    @ParameterizedTest(name = "Even non-power-of-2: {0} leaves")
    @ValueSource(
            ints = {
                6, 10, 12, 14, 18, 20, 22, 24, 26, 28, 30, 34, 48, 50, 62, 66, 98, 100, 126, 130, 254, 258, 500, 510,
                514, 1000, 1022, 1026
            })
    @DisplayName("Even non-power-of-2 leaf counts should produce matching root hashes")
    void testEvenNonPowerOfTwoLeafCounts(int numLeaves) {
        assertHashersMatch(numLeaves, RANDOM_SEED);
    }

    // ========== Fibonacci Leaf Count Tests ==========

    /**
     * Tests Fibonacci numbers which create interesting tree shapes.
     *
     * <p>Fibonacci numbers have the property that F(n) = F(n-1) + F(n-2), creating trees where
     * subtrees have sizes related by the golden ratio. This tests the algorithm with naturally
     * occurring size distributions.
     */
    @ParameterizedTest(name = "Fibonacci: {0} leaves")
    @ValueSource(ints = {1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987})
    @DisplayName("Fibonacci leaf counts should produce matching root hashes")
    void testFibonacciLeafCounts(int numLeaves) {
        assertHashersMatch(numLeaves, RANDOM_SEED);
    }

    // ========== Power-of-2 Boundary Tests ==========

    /**
     * Provides test arguments for boundary cases around powers of 2.
     *
     * @return stream of (leafCount, description) pairs
     */
    static Stream<Arguments> powerOfTwoBoundaries() {
        return IntStream.of(2, 4, 8, 16, 32, 64, 128, 256, 512, 1024)
                .boxed()
                .flatMap(p -> Stream.of(
                        Arguments.of(p - 1, "2^" + Integer.numberOfTrailingZeros(p) + " - 1"),
                        Arguments.of(p, "2^" + Integer.numberOfTrailingZeros(p)),
                        Arguments.of(p + 1, "2^" + Integer.numberOfTrailingZeros(p) + " + 1")));
    }

    /**
     * Tests boundary cases around powers of 2 (n-1, n, n+1 for each power of 2).
     *
     * <p>Boundary cases are critical because:
     * <ul>
     *   <li>n-1: Maximum complexity for that tree height</li>
     *   <li>n: Perfect tree (simplest case)</li>
     *   <li>n+1: Minimum complexity for next tree height</li>
     * </ul>
     */
    @ParameterizedTest(name = "{1} = {0} leaves")
    @MethodSource("powerOfTwoBoundaries")
    @DisplayName("Boundary cases around powers of 2 should produce matching root hashes")
    void testPowerOfTwoBoundaries(int numLeaves, String description) {
        assertHashersMatch(numLeaves, RANDOM_SEED);
    }

    // ========== Random Configuration Tests ==========

    /**
     * Provides test configurations with different leaf counts and seeds.
     *
     * @return stream of (leafCount, seed, description) tuples
     */
    static Stream<Arguments> randomTreeConfigurations() {
        return Stream.of(
                Arguments.of(100, 1L, "100 leaves, seed 1"),
                Arguments.of(100, 2L, "100 leaves, seed 2"),
                Arguments.of(100, 42L, "100 leaves, seed 42"),
                Arguments.of(500, 1L, "500 leaves, seed 1"),
                Arguments.of(500, 100L, "500 leaves, seed 100"),
                Arguments.of(1000, 1L, "1000 leaves, seed 1"),
                Arguments.of(1000, 999L, "1000 leaves, seed 999"),
                Arguments.of(1024, 1L, "1024 leaves, seed 1"),
                Arguments.of(1024, 1024L, "1024 leaves, seed 1024"),
                Arguments.of(1023, 1L, "1023 leaves, seed 1"),
                Arguments.of(1025, 1L, "1025 leaves, seed 1"));
    }

    /**
     * Tests various tree sizes with different random seeds to ensure the algorithm works
     * correctly for different data patterns.
     */
    @ParameterizedTest(name = "{2}")
    @MethodSource("randomTreeConfigurations")
    @DisplayName("Random tree configurations with various seeds should produce matching root hashes")
    void testRandomTreeConfigurations(int numLeaves, long seed, String description) {
        assertHashersMatch(numLeaves, seed);
    }

    // ========== Variable Leaf Size Tests ==========

    /**
     * Tests trees with variable-sized leaves (1 byte to 8 KB each).
     *
     * <p>This verifies that the hashing algorithm correctly handles leaves of different sizes,
     * which is important since real-world block data varies in size.
     */
    @Test
    @DisplayName("Trees with variable-sized leaves (1B to 8KB) should produce matching root hashes")
    void testVariableSizedLeaves() {
        Random random = new Random(RANDOM_SEED);
        StreamingHasher streaming = new StreamingHasher();
        InMemoryTreeHasher inMemory = new InMemoryTreeHasher();

        for (int i = 0; i < 500; i++) {
            int leafSize = 1 + random.nextInt(8192);
            byte[] leafData = new byte[leafSize];
            random.nextBytes(leafData);

            streaming.addLeaf(leafData);
            inMemory.addLeaf(leafData);
        }

        assertArrayEquals(
                streaming.computeRootHash(),
                inMemory.computeRootHash(),
                "500 variable-sized leaves (1B-8KB) should produce matching root hashes");
    }

    // ========== Identical Leaves Tests ==========

    /**
     * Tests trees where all leaves contain identical data.
     *
     * <p>This is an edge case where the only differentiation between leaf positions is their
     * position in the tree, not their content.
     */
    @ParameterizedTest(name = "Identical leaves: {0} copies")
    @ValueSource(ints = {2, 3, 4, 7, 8, 15, 16, 31, 32, 100, 127, 128, 255, 256, 500, 512, 1000, 1024})
    @DisplayName("Trees with all identical leaves should produce matching root hashes")
    void testIdenticalLeaves(int numLeaves) {
        byte[] identicalData = "This is identical leaf data for all leaves".getBytes();
        StreamingHasher streaming = new StreamingHasher();
        InMemoryTreeHasher inMemory = new InMemoryTreeHasher();

        for (int i = 0; i < numLeaves; i++) {
            streaming.addLeaf(identicalData);
            inMemory.addLeaf(identicalData);
        }

        assertArrayEquals(
                streaming.computeRootHash(),
                inMemory.computeRootHash(),
                numLeaves + " identical leaves should produce matching root hashes");
    }

    // ========== Empty Leaf Data Tests ==========

    /**
     * Tests trees with empty (zero-byte) leaf data.
     *
     * <p>Empty leaves are a valid edge case that must be handled correctly. The leaf hash
     * is computed from the empty byte array prefixed with the leaf marker.
     */
    @ParameterizedTest(name = "Empty leaves: {0} count")
    @ValueSource(ints = {1, 2, 3, 4, 7, 8, 15, 16, 31, 32, 100})
    @DisplayName("Trees with empty (0-byte) leaf data should produce matching root hashes")
    void testEmptyLeaves(int numLeaves) {
        byte[] emptyData = new byte[0];
        StreamingHasher streaming = new StreamingHasher();
        InMemoryTreeHasher inMemory = new InMemoryTreeHasher();

        for (int i = 0; i < numLeaves; i++) {
            streaming.addLeaf(emptyData);
            inMemory.addLeaf(emptyData);
        }

        assertArrayEquals(
                streaming.computeRootHash(),
                inMemory.computeRootHash(),
                numLeaves + " empty leaves should produce matching root hashes");
    }

    // ========== Large Leaf Tests ==========

    /**
     * Tests trees with large (1 MB) leaves.
     *
     * <p>This verifies that the hashing algorithm correctly handles large data without
     * memory or performance issues.
     */
    @Test
    @DisplayName("Trees with large leaves (1 MB each) should produce matching root hashes")
    void testLargeLeaves() {
        Random random = new Random(RANDOM_SEED);
        StreamingHasher streaming = new StreamingHasher();
        InMemoryTreeHasher inMemory = new InMemoryTreeHasher();

        for (int i = 0; i < 10; i++) {
            byte[] largeData = new byte[1024 * 1024];
            random.nextBytes(largeData);
            streaming.addLeaf(largeData);
            inMemory.addLeaf(largeData);
        }

        assertArrayEquals(
                streaming.computeRootHash(),
                inMemory.computeRootHash(),
                "10 large leaves (1 MB each) should produce matching root hashes");
    }

    // ========== Incremental Verification Tests ==========

    /**
     * Verifies that root hashes match after every single leaf addition for 100 leaves.
     *
     * <p>This is the most thorough test of incremental tree building correctness.
     */
    @Test
    @DisplayName("Root hash should match after each of 100 leaf additions")
    void testIntermediateRootHashesMatch() {
        assertHashersMatchIncrementally(100, RANDOM_SEED);
    }

    /**
     * Tests incremental root hash matching for various tree sizes.
     *
     * <p>Each test verifies that the root hash matches after EVERY leaf addition,
     * not just at the final count.
     */
    @ParameterizedTest(name = "Incremental verification: {0} leaves")
    @ValueSource(ints = {1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 100, 127, 128, 129, 255, 256, 257})
    @DisplayName("Root hash should match after each leaf addition for various tree sizes")
    void testIncrementalRootHashMatching(int numLeaves) {
        assertHashersMatchIncrementally(numLeaves, RANDOM_SEED);
    }

    /**
     * Provides test configurations for incremental verification with various seeds.
     *
     * @return stream of (leafCount, seed, description) tuples
     */
    static Stream<Arguments> incrementalTestConfigurations() {
        return Stream.of(
                Arguments.of(50, 1L, "50 leaves, seed 1"),
                Arguments.of(50, 42L, "50 leaves, seed 42"),
                Arguments.of(100, 123L, "100 leaves, seed 123"),
                Arguments.of(200, 456L, "200 leaves, seed 456"),
                Arguments.of(512, 789L, "512 leaves, seed 789"));
    }

    /**
     * Tests incremental verification with different random seeds.
     */
    @ParameterizedTest(name = "Incremental with seed {1}: {0} leaves")
    @MethodSource("incrementalTestConfigurations")
    @DisplayName("Incremental root hash verification should work with various seeds")
    void testIncrementalWithDifferentSeeds(int numLeaves, long seed, String description) {
        assertHashersMatchIncrementally(numLeaves, seed);
    }

    /**
     * Verifies that the pending subtree count matches the expected pattern.
     *
     * <p>For n leaves, the number of pending subtrees equals the population count (number of 1-bits)
     * in the binary representation of n. This is a fundamental property of the streaming algorithm.
     */
    @Test
    @DisplayName("Pending subtree count should equal popcount of leaf count")
    void testPendingSubtreeCount() {
        InMemoryTreeHasher inMemory = new InMemoryTreeHasher();

        for (int i = 1; i <= 100; i++) {
            inMemory.addLeaf(("leaf " + i).getBytes());
            int expectedPending = Integer.bitCount(i);
            assertEquals(
                    expectedPending,
                    inMemory.pendingSubtreeCount(),
                    "After " + i + " leaves, pending subtree count should equal popcount(" + i + ") = "
                            + expectedPending);
        }
    }

    // ========== Stress Tests ==========

    /**
     * Stress test with 2048 random leaves.
     */
    @Test
    @DisplayName("Large tree with 2048 random leaves should produce matching root hashes")
    void testLargeTreeWith2048Leaves() {
        assertHashersMatch(2048, RANDOM_SEED);
    }

    /**
     * Stress test with 4096 random leaves.
     */
    @Test
    @DisplayName("Large tree with 4096 random leaves should produce matching root hashes")
    void testLargeTreeWith4096Leaves() {
        assertHashersMatch(4096, RANDOM_SEED);
    }

    // ========== Build Order Tests ==========

    /**
     * Verifies that building trees with the same data in the same order produces identical results,
     * regardless of which hasher is used.
     */
    @Test
    @DisplayName("Sequential building with same data should produce identical root hashes")
    void testSequentialVsBatchBuilding() {
        Random random1 = new Random(RANDOM_SEED);
        Random random2 = new Random(RANDOM_SEED);

        StreamingHasher streaming = new StreamingHasher();
        for (int i = 0; i < 500; i++) {
            byte[] data = new byte[100];
            random1.nextBytes(data);
            streaming.addLeaf(data);
        }

        InMemoryTreeHasher inMemory = new InMemoryTreeHasher();
        for (int i = 0; i < 500; i++) {
            byte[] data = new byte[100];
            random2.nextBytes(data);
            inMemory.addLeaf(data);
        }

        assertArrayEquals(
                streaming.computeRootHash(),
                inMemory.computeRootHash(),
                "Sequential building with same data (500 leaves) should produce identical root hashes");
    }

    // ========== Helper Methods ==========

    /**
     * Asserts that StreamingHasher and InMemoryTreeHasher produce identical root hashes for the same input data.
     * Only verifies the final root hash after all leaves are added.
     *
     * @param numLeaves the number of leaves to add
     * @param seed the random seed for reproducible data generation
     */
    private void assertHashersMatch(int numLeaves, long seed) {
        Random random = new Random(seed);
        StreamingHasher streaming = new StreamingHasher();
        InMemoryTreeHasher inMemory = new InMemoryTreeHasher();

        for (int i = 0; i < numLeaves; i++) {
            int leafSize = MIN_LEAF_SIZE + random.nextInt(MAX_LEAF_SIZE - MIN_LEAF_SIZE + 1);
            byte[] leafData = new byte[leafSize];
            random.nextBytes(leafData);

            streaming.addLeaf(leafData);
            inMemory.addLeaf(leafData);
        }

        assertEquals(numLeaves, streaming.leafCount(), "StreamingHasher should have " + numLeaves + " leaves");
        assertEquals(numLeaves, inMemory.leafCount(), "InMemoryTreeHasher should have " + numLeaves + " leaves");

        byte[] streamingRoot = streaming.computeRootHash();
        byte[] inMemoryRoot = inMemory.computeRootHash();

        assertArrayEquals(
                streamingRoot,
                inMemoryRoot,
                "Root hash mismatch: StreamingHasher and InMemoryTreeHasher should produce identical hashes for "
                        + numLeaves + " leaves with seed " + seed);
    }

    /**
     * Asserts that StreamingHasher and InMemoryTreeHasher produce identical root hashes after EACH leaf is added.
     * This verifies the incremental tree building is correct at every step.
     *
     * @param numLeaves the number of leaves to add
     * @param seed the random seed for reproducible data generation
     */
    private void assertHashersMatchIncrementally(int numLeaves, long seed) {
        Random random = new Random(seed);
        StreamingHasher streaming = new StreamingHasher();
        InMemoryTreeHasher inMemory = new InMemoryTreeHasher();

        for (int i = 0; i < numLeaves; i++) {
            int leafSize = MIN_LEAF_SIZE + random.nextInt(MAX_LEAF_SIZE - MIN_LEAF_SIZE + 1);
            byte[] leafData = new byte[leafSize];
            random.nextBytes(leafData);

            streaming.addLeaf(leafData);
            inMemory.addLeaf(leafData);

            int leafNum = i + 1;

            assertEquals(
                    streaming.leafCount(),
                    inMemory.leafCount(),
                    "Leaf count mismatch after adding leaf " + leafNum + " of " + numLeaves);

            byte[] streamingRoot = streaming.computeRootHash();
            byte[] inMemoryRoot = inMemory.computeRootHash();

            assertArrayEquals(
                    streamingRoot,
                    inMemoryRoot,
                    "Root hash mismatch after adding leaf " + leafNum + " of " + numLeaves
                            + ": StreamingHasher and InMemoryTreeHasher should match (seed=" + seed + ")");
        }
    }
}
