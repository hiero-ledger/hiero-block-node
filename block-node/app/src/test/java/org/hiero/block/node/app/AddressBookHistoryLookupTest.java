// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import java.util.List;
import java.util.NavigableMap;
import org.hiero.block.api.RangedAddressBookHistory;
import org.hiero.block.api.RangedNodeAddressBook;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/// Unit tests for {@link AddressBookHistoryLookup}.
///
/// Covers the O(log n) index lookup under all boundary conditions:
/// exact range boundaries, mid-range, open-ended sentinel, gaps between eras,
/// blocks before any era, and the single-entry backward-compatibility case.
class AddressBookHistoryLookupTest {

    private NodeAddressBook era1Book;
    private NodeAddressBook era2Book;
    private NodeAddressBook era3Book;

    @BeforeEach
    void setUp() {
        era1Book = addressBook("aaa");
        era2Book = addressBook("bbb");
        era3Book = addressBook("ccc");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static NodeAddressBook addressBook(String rsaKey) {
        return NodeAddressBook.newBuilder()
                .nodeAddress(List.of(
                        NodeAddress.newBuilder().nodeId(1L).rsaPubKey(rsaKey).build()))
                .build();
    }

    private static RangedNodeAddressBook ranged(NodeAddressBook book, long start, long end) {
        return RangedNodeAddressBook.newBuilder()
                .addressBook(book)
                .startBlock(start)
                .endBlock(end)
                .build();
    }

    private static NavigableMap<Long, RangedNodeAddressBook> index(RangedNodeAddressBook... entries) {
        return AddressBookHistoryLookup.buildIndex(RangedAddressBookHistory.newBuilder()
                .addressBooks(List.of(entries))
                .build());
    }

    // -------------------------------------------------------------------------
    // buildIndex
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("buildIndex")
    class BuildIndex {

        @Test
        @DisplayName("returns empty map for empty history")
        void emptyHistory() {
            final var idx = AddressBookHistoryLookup.buildIndex(RangedAddressBookHistory.DEFAULT);
            assertTrue(idx.isEmpty());
        }

        @Test
        @DisplayName("indexes all entries keyed by startBlock ascending")
        void multipleEntries() {
            final var idx = index(ranged(era1Book, 0, 100), ranged(era2Book, 101, 200), ranged(era3Book, 201, 0));
            assertEquals(3, idx.size());
            assertEquals(List.of(0L, 101L, 201L), List.copyOf(idx.keySet()));
        }
    }

    // -------------------------------------------------------------------------
    // findAddressBookForBlock — before any era
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("findAddressBookForBlock — before any era")
    class BeforeAnyEra {

        @Test
        @DisplayName("block before earliest startBlock returns null")
        void blockBeforeFirstEra() {
            final var idx = index(ranged(era1Book, 100, 200));
            assertNull(AddressBookHistoryLookup.findAddressBookForBlock(idx, 99));
        }

        @Test
        @DisplayName("block 0 on empty index returns null")
        void emptyIndex() {
            final var idx = index();
            assertNull(AddressBookHistoryLookup.findAddressBookForBlock(idx, 0));
        }
    }

    // -------------------------------------------------------------------------
    // findAddressBookForBlock — exact boundaries
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("findAddressBookForBlock — exact boundaries")
    class ExactBoundaries {

        @Test
        @DisplayName("exact startBlock returns the correct book")
        void exactStart() {
            final var idx = index(ranged(era1Book, 100, 200));
            final NodeAddressBook result = AddressBookHistoryLookup.findAddressBookForBlock(idx, 100);
            assertNotNull(result);
            assertEquals(era1Book, result);
        }

        @Test
        @DisplayName("exact endBlock returns the correct book")
        void exactEnd() {
            final var idx = index(ranged(era1Book, 100, 200));
            final NodeAddressBook result = AddressBookHistoryLookup.findAddressBookForBlock(idx, 200);
            assertNotNull(result);
            assertEquals(era1Book, result);
        }

        @Test
        @DisplayName("one past endBlock returns null")
        void onePastEnd() {
            final var idx = index(ranged(era1Book, 100, 200));
            assertNull(AddressBookHistoryLookup.findAddressBookForBlock(idx, 201));
        }
    }

    // -------------------------------------------------------------------------
    // findAddressBookForBlock — mid-range
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("findAddressBookForBlock — mid-range")
    class MidRange {

        @Test
        @DisplayName("mid-range block returns the correct book")
        void midRange() {
            final var idx = index(ranged(era1Book, 0, 100), ranged(era2Book, 101, 300), ranged(era3Book, 301, 0));
            final NodeAddressBook result = AddressBookHistoryLookup.findAddressBookForBlock(idx, 200);
            assertNotNull(result);
            assertEquals(era2Book, result);
        }

        @Test
        @DisplayName("first block of second era returns era2 book")
        void firstBlockOfSecondEra() {
            final var idx = index(ranged(era1Book, 0, 100), ranged(era2Book, 101, 300));
            assertEquals(era2Book, AddressBookHistoryLookup.findAddressBookForBlock(idx, 101));
        }
    }

    // -------------------------------------------------------------------------
    // findAddressBookForBlock — open-ended sentinel (endBlock == 0)
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("findAddressBookForBlock — open-ended sentinel")
    class OpenEndedSentinel {

        @Test
        @DisplayName("block beyond last closed entry covered by open-ended entry")
        void openEndedCoversLargeBlock() {
            final var idx = index(ranged(era1Book, 0, 1000), ranged(era2Book, 1001, 0));
            assertEquals(era2Book, AddressBookHistoryLookup.findAddressBookForBlock(idx, 999_999L));
        }

        @Test
        @DisplayName("single open-ended entry covers startBlock itself")
        void singleOpenEndedCoversStart() {
            final var idx = index(ranged(era1Book, 0, 0));
            assertEquals(era1Book, AddressBookHistoryLookup.findAddressBookForBlock(idx, 0));
        }

        @Test
        @DisplayName("block before single open-ended entry returns null")
        void blockBeforeOpenEndedEntry() {
            final var idx = index(ranged(era1Book, 50, 0));
            assertNull(AddressBookHistoryLookup.findAddressBookForBlock(idx, 49));
        }
    }

    // -------------------------------------------------------------------------
    // findAddressBookForBlock — gaps between eras
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("findAddressBookForBlock — gaps between eras")
    class GapsBetweenEras {

        @Test
        @DisplayName("block in gap between two eras returns null")
        void blockInGap() {
            // era1: 0–100, era2: 200–300 — gap: 101–199
            final var idx = index(ranged(era1Book, 0, 100), ranged(era2Book, 200, 300));
            assertNull(AddressBookHistoryLookup.findAddressBookForBlock(idx, 150));
        }

        @Test
        @DisplayName("block exactly at gap start (endBlock+1) returns null")
        void blockAtGapStart() {
            final var idx = index(ranged(era1Book, 0, 100), ranged(era2Book, 200, 300));
            assertNull(AddressBookHistoryLookup.findAddressBookForBlock(idx, 101));
        }
    }

    // -------------------------------------------------------------------------
    // Backward-compatibility: single-entry history (wraps single NodeAddressBook)
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("Backward-compatibility: single-entry open-ended history")
    class SingleEntryBackwardCompat {

        @Test
        @DisplayName("any block number is covered by a single open-ended entry starting at 0")
        void coversAllBlocks() {
            final var idx = index(ranged(era1Book, 0, 0));
            assertNotNull(AddressBookHistoryLookup.findAddressBookForBlock(idx, 0));
            assertNotNull(AddressBookHistoryLookup.findAddressBookForBlock(idx, 1));
            assertNotNull(AddressBookHistoryLookup.findAddressBookForBlock(idx, Long.MAX_VALUE));
        }
    }
}
