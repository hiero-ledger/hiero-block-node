// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.node.base.NodeAddressBook;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.hiero.block.api.RangedNodeAddressBook;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Unit tests for [RangedAddressBookComparator].
class RangedAddressBookComparatorTest {

    private final RangedAddressBookComparator comparator = new RangedAddressBookComparator();

    /// An era with a non-null (usable) address book.
    private static RangedNodeAddressBook usableEra(final long startBlock, final long endBlock) {
        return RangedNodeAddressBook.newBuilder()
                .addressBook(NodeAddressBook.newBuilder().build())
                .startBlock(startBlock)
                .endBlock(endBlock)
                .build();
    }

    /// An era with a null (unusable) address book.
    private static RangedNodeAddressBook nullEra(final long startBlock, final long endBlock) {
        return RangedNodeAddressBook.newBuilder()
                .startBlock(startBlock)
                .endBlock(endBlock)
                .build();
    }

    /// Returns the last element after sorting the supplied eras with the comparator.
    private RangedNodeAddressBook sortedLast(final RangedNodeAddressBook... eras) {
        return List.of(eras).stream().sorted(comparator).toList().getLast();
    }

    @Test
    @DisplayName("Higher startBlock sorts last")
    void higherStartBlockSortsLast() {
        final RangedNodeAddressBook last = sortedLast(usableEra(0, 99), usableEra(100, -1), usableEra(50, 99));
        assertEquals(100L, last.startBlock());
    }

    @Test
    @DisplayName("Open-ended era (endBlock == -1) sorts after a bounded era with the same startBlock")
    void openEndedSortsAfterBoundedOnStartBlockTie() {
        final RangedNodeAddressBook bounded = usableEra(100, 200);
        final RangedNodeAddressBook openEnded = usableEra(100, -1);
        assertTrue(comparator.compare(bounded, openEnded) < 0, "bounded must sort before open-ended");
        assertEquals(-1L, sortedLast(bounded, openEnded).endBlock());
    }

    @Test
    @DisplayName("Two bounded eras with equal startBlock order by endBlock ascending")
    void boundedErasOrderByEndBlockAscending() {
        final RangedNodeAddressBook lowEnd = usableEra(100, 200);
        final RangedNodeAddressBook highEnd = usableEra(100, 300);
        assertTrue(comparator.compare(lowEnd, highEnd) < 0);
        assertEquals(300L, sortedLast(lowEnd, highEnd).endBlock());
    }

    @Test
    @DisplayName("Block numbers are compared unsigned: a startBlock of -1 is the largest, sorting last")
    void startBlockComparedUnsigned() {
        // As a signed long, -1 < 5; as an unsigned long, -1 is the maximum value and must sort last.
        final RangedNodeAddressBook maxUnsignedStart = usableEra(-1L, -1L);
        final RangedNodeAddressBook smallStart = usableEra(5L, 99L);
        assertTrue(comparator.compare(smallStart, maxUnsignedStart) < 0, "unsigned: 5 < (2^64 - 1)");
        assertEquals(-1L, sortedLast(smallStart, maxUnsignedStart).startBlock());
    }

    @Test
    @DisplayName("A null-address-book era sorts before a usable era regardless of startBlock")
    void nullBookEraSortsBeforeUsableRegardlessOfStartBlock() {
        // The null-book era has the higher startBlock but must still sort first, so getLast() is usable.
        final RangedNodeAddressBook nullHigh = nullEra(100, -1);
        final RangedNodeAddressBook usableLow = usableEra(0, -1);
        assertTrue(comparator.compare(nullHigh, usableLow) < 0, "null-book era must sort before a usable era");
        final RangedNodeAddressBook last = sortedLast(nullHigh, usableLow);
        assertNotNull(last.addressBook(), "the last (newest) era must have a usable address book");
        assertEquals(0L, last.startBlock());
    }

    @Test
    @DisplayName("A realistic multi-era history selects the greatest-startBlock era as last")
    void multiEraSelectsGreatestStartBlock() {
        final RangedNodeAddressBook last = sortedLast(usableEra(0, 999), usableEra(1000, 1999), usableEra(2000, -1));
        assertEquals(2000L, last.startBlock());
        assertEquals(-1L, last.endBlock());
    }

    @Test
    @DisplayName("Sorting a shuffled list is deterministic and orders by startBlock ascending")
    void shuffledListSortsDeterministically() {
        final List<RangedNodeAddressBook> expected =
                List.of(usableEra(0, 99), usableEra(100, 199), usableEra(200, 299), usableEra(300, -1));
        final List<RangedNodeAddressBook> shuffled = new ArrayList<>(expected);
        Collections.shuffle(shuffled);
        final List<RangedNodeAddressBook> sorted =
                shuffled.stream().sorted(comparator).toList();
        assertEquals(expected, sorted);
    }
}
