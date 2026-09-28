// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session.eviction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.hiero.block.node.block.verification.session.BlockVerificationSession.SessionKey;
import org.hiero.block.node.block.verification.session.SessionPriority;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/// Tests for the [GapAwareEvictionPolicy].
///
/// Every test fabricates an [EvictionSnapshot] directly, no sessions run. The
/// policy is a pure function, so each test states the buffer content, the
/// last verified block, the limit and the protected keys (the publisher
/// sessions still receiving their block), and asserts exactly which keys
/// come back, in which order.
@DisplayName("Gap Aware Eviction Policy Tests")
class GapAwareEvictionPolicyTest {
    /// The policy under test, stateless.
    private final GapAwareEvictionPolicy toTest = new GapAwareEvictionPolicy();

    /// Build a high priority publisher session snapshot.
    private static SessionSnapshot high(final long blockNumber, final long uniqueId) {
        return new SessionSnapshot(new SessionKey(blockNumber, uniqueId), SessionPriority.HIGH, BlockSource.PUBLISHER);
    }

    /// Build a low priority backfill session snapshot.
    private static SessionSnapshot low(final long blockNumber, final long uniqueId) {
        return new SessionSnapshot(new SessionKey(blockNumber, uniqueId), SessionPriority.LOW, BlockSource.BACKFILL);
    }

    /// Build a snapshot with every block ordered from block zero and all sources ordered.
    private static EvictionSnapshot snapshot(
            final List<SessionSnapshot> sessions,
            final long lastVerified,
            final int limit,
            final SessionSnapshot... protectedSessions) {
        return snapshot(sessions, lastVerified, 0L, true, limit, protectedSessions);
    }

    /// Build a snapshot with explicit ordering settings.
    private static EvictionSnapshot snapshot(
            final List<SessionSnapshot> sessions,
            final long lastVerified,
            final long firstOrderedBlock,
            final boolean allSourcesRequireOrdering,
            final int limit,
            final SessionSnapshot... protectedSessions) {
        final Set<SessionKey> protectedKeys = Set.of(
                Arrays.stream(protectedSessions).map(SessionSnapshot::key).toArray(SessionKey[]::new));
        return new EvictionSnapshot(
                sessions, lastVerified, firstOrderedBlock, allSourcesRequireOrdering, limit, protectedKeys);
    }

    /// Tests for the trigger condition and the input contract.
    @Nested
    @DisplayName("Contract Tests")
    class ContractTests {
        /// This test aims to assert that the policy selects nothing while the
        /// buffer is at or below its limit, even when every session is
        /// waiting for an earlier block.
        @Test
        @DisplayName("selects nothing when the buffer is within its limit")
        void testSelectsNothingWithinLimit() {
            final List<SessionSnapshot> sessions = List.of(high(102, 0), high(103, 1), low(110, 2));
            assertThat(toTest.selectVictims(snapshot(sessions, 100, 3))).isEmpty();
            assertThat(toTest.selectVictims(snapshot(sessions, 100, 4))).isEmpty();
        }

        /// This test aims to assert that a null snapshot is rejected up front
        /// with a [NullPointerException] instead of being silently tolerated.
        @Test
        @DisplayName("rejects a null snapshot")
        void testRejectsNullSnapshot() {
            assertThatNullPointerException().isThrownBy(() -> toTest.selectVictims(null));
        }

        /// This test aims to assert that the snapshot itself refuses a non
        /// positive limit, since a limit of zero would make every session a
        /// victim and a negative one is meaningless.
        @Test
        @DisplayName("snapshot rejects a non positive limit")
        void testSnapshotRejectsNonPositiveLimit() {
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> new EvictionSnapshot(List.of(), 100, 0, true, 0, Set.of()));
        }
    }

    /// Tests asserting that sessions which will complete on their own are never evicted.
    @Nested
    @DisplayName("Non Waiting Sessions Are Never Evicted")
    class NonWaitingSessionTests {
        /// This test aims to assert that low priority sessions for blocks below
        /// the next expected block (historical backfill) are never evicted, and
        /// that the highest waiting publisher session goes instead, even though
        /// low priority sessions are normally evicted first.
        @Test
        @DisplayName("historical backfill sessions below the next expected block are kept")
        void testHistoricalBackfillKept() {
            final List<SessionSnapshot> sessions = List.of(low(5, 0), low(6, 1), low(7, 2), high(102, 3), high(103, 4));
            final List<SessionKey> victims = toTest.selectVictims(snapshot(sessions, 100, 4));
            assertThat(victims).containsExactly(new SessionKey(103, 4));
        }

        /// This test aims to assert that publisher sessions at or below the
        /// last verified block (duplicates) and the session for the next
        /// expected block (the head of the chain) are never evicted, and that
        /// the highest waiting publisher session is chosen instead.
        @Test
        @DisplayName("duplicates and the chain head are kept")
        void testDuplicatesAndHeadKept() {
            final List<SessionSnapshot> sessions = List.of(high(100, 0), high(101, 1), high(102, 2), high(103, 3));
            final List<SessionKey> victims = toTest.selectVictims(snapshot(sessions, 100, 3));
            assertThat(victims).containsExactly(new SessionKey(103, 3));
        }

        /// This test aims to assert that when sources other than the publisher
        /// are not subject to ordering, low priority sessions far ahead are not
        /// waiting and are therefore kept, while a waiting publisher session is
        /// evicted.
        @Test
        @DisplayName("unordered sources are kept when all sources require ordering is off")
        void testUnorderedSourcesKept() {
            final List<SessionSnapshot> sessions = List.of(low(150, 0), low(160, 1), high(102, 2), high(103, 3));
            final List<SessionKey> victims = toTest.selectVictims(snapshot(sessions, 100, 0L, false, 3));
            assertThat(victims).containsExactly(new SessionKey(103, 3));
        }

        /// This test aims to assert that sessions for blocks below the first
        /// ordered block never wait for order and are therefore never evicted.
        @Test
        @DisplayName("sessions below the first ordered block are kept")
        void testSessionsBelowFirstOrderedBlockKept() {
            final List<SessionSnapshot> sessions = List.of(high(500, 0), high(600, 1), high(1002, 2), high(1003, 3));
            final List<SessionKey> victims = toTest.selectVictims(snapshot(sessions, 1000, 1000L, true, 3));
            assertThat(victims).containsExactly(new SessionKey(1003, 3));
        }

        /// This test aims to assert that when only sessions that will complete
        /// on their own are over the limit, the policy returns no victims and
        /// lets the buffer overshoot transiently.
        @Test
        @DisplayName("returns nothing when only non waiting sessions are over the limit")
        void testOvershootWithOnlyNonWaitingSessions() {
            final List<SessionSnapshot> sessions = List.of(low(5, 0), low(6, 1), low(7, 2));
            assertThat(toTest.selectVictims(snapshot(sessions, 100, 2))).isEmpty();
        }
    }

    /// Tests for the first tier: a low priority session at the top of the waiting range.
    @Nested
    @DisplayName("Tier One Tests")
    class TierOneTests {
        /// This test aims to assert that a waiting low priority session at the
        /// top of the waiting range, which no other session depends on, is
        /// evicted before any publisher session.
        @Test
        @DisplayName("low priority session at the top of the waiting range is evicted before publisher sessions")
        void testLowAtTopEvictedFirst() {
            final List<SessionSnapshot> sessions = List.of(high(102, 0), high(103, 1), high(104, 2), low(110, 3));
            final List<SessionKey> victims = toTest.selectVictims(snapshot(sessions, 100, 3));
            assertThat(victims).containsExactly(new SessionKey(110, 3));
        }

        /// This test aims to assert that a low priority session filling a gap
        /// that a higher publisher session waits for is not evicted while a
        /// publisher session can be evicted instead.
        @Test
        @DisplayName("low priority session filling a gap is kept while a publisher alternative exists")
        void testLowFillingGapKept() {
            final List<SessionSnapshot> sessions = List.of(low(102, 0), high(103, 1), high(104, 2), high(105, 3));
            final List<SessionKey> victims = toTest.selectVictims(snapshot(sessions, 100, 3));
            assertThat(victims).containsExactly(new SessionKey(105, 3));
        }

        /// This test aims to assert that a low priority session so far ahead
        /// that it cannot be released before the whole buffer has turned over
        /// is evicted first even when it is the newest session, the one whose
        /// activation triggered the round: the newest session enjoys no
        /// protection, so a publisher session is never evicted on its behalf.
        @Test
        @DisplayName("far ahead low priority session is evicted even when it is the newest")
        void testFarAheadLowEvictedEvenWhenNewest() {
            final List<SessionSnapshot> sessions = List.of(high(102, 0), high(103, 1), high(104, 2), low(5000, 3));
            final List<SessionKey> victims = toTest.selectVictims(snapshot(sessions, 100, 3));
            assertThat(victims).containsExactly(new SessionKey(5000, 3));
        }

        /// This test aims to assert that a low priority session below a
        /// protected publisher session at the top of the waiting range is not
        /// a first tier candidate, since the protected session depends on it:
        /// the top of the range counts protected sessions too.
        @Test
        @DisplayName("the top of the waiting range counts protected sessions")
        void testTopCountsProtectedSessions() {
            final SessionSnapshot incomplete = high(105, 3);
            final List<SessionSnapshot> sessions = List.of(low(102, 0), high(103, 1), low(104, 2), incomplete);
            final List<SessionKey> victims = toTest.selectVictims(snapshot(sessions, 100, 3, incomplete));
            assertThat(victims).containsExactly(new SessionKey(103, 1));
        }
    }

    /// Tests for the second tier: publisher sessions that received their complete block.
    @Nested
    @DisplayName("Tier Two Tests")
    class TierTwoTests {
        /// This test aims to assert that among publisher sessions the highest
        /// one not still receiving its block is evicted, keeping the base of
        /// the chain intact so it releases the moment the missing block arrives.
        @Test
        @DisplayName("highest non protected publisher session is evicted")
        void testHighestNonProtectedPublisherEvicted() {
            final SessionSnapshot incomplete = high(105, 3);
            final List<SessionSnapshot> sessions = List.of(high(102, 0), high(103, 1), high(104, 2), incomplete);
            final List<SessionKey> victims = toTest.selectVictims(snapshot(sessions, 100, 3, incomplete));
            assertThat(victims).containsExactly(new SessionKey(104, 2));
        }

        /// This test aims to assert that a complete publisher session at the
        /// top of the waiting range is evicted even when it is the newest
        /// session, the one whose activation triggered the round, since it
        /// enjoys no protection once its complete block was received.
        @Test
        @DisplayName("complete publisher session at the top is evicted even when it is the newest")
        void testCompletePublisherAtTopEvictedEvenWhenNewest() {
            final List<SessionSnapshot> sessions = List.of(high(102, 0), high(103, 1), high(104, 2), high(105, 3));
            final List<SessionKey> victims = toTest.selectVictims(snapshot(sessions, 100, 3));
            assertThat(victims).containsExactly(new SessionKey(105, 3));
        }

        /// This test aims to assert that when every waiting session is protected
        /// the policy returns no victims instead of violating the protection.
        @Test
        @DisplayName("returns nothing when every waiting session is protected")
        void testAllProtected() {
            final SessionSnapshot first = high(102, 0);
            final SessionSnapshot second = high(103, 1);
            final List<SessionKey> victims =
                    toTest.selectVictims(snapshot(List.of(first, second), 100, 1, first, second));
            assertThat(victims).isEmpty();
        }

        /// This test aims to assert the reverse order scenario from the design
        /// call: blocks 10, 9, 8 fill a buffer of three and block 7 arrives;
        /// the highest block is evicted, the newest and lowest block is kept,
        /// and the buffer never grows past its limit.
        @Test
        @DisplayName("reverse order keeps the newest lowest block and evicts the highest")
        void testReverseOrder() {
            final List<SessionSnapshot> sessions = List.of(high(10, 0), high(9, 1), high(8, 2), high(7, 3));
            final List<SessionKey> victims = toTest.selectVictims(snapshot(sessions, 5, 3));
            assertThat(victims).containsExactly(new SessionKey(10, 0));
        }

        /// This test aims to assert that a low priority block arriving in
        /// reverse order inside the gap a parked publisher chain waits for is
        /// kept, and the top of the publisher chain is evicted instead.
        @Test
        @DisplayName("reverse order backfill inside the gap is kept")
        void testReverseOrderBackfillInsideGapKept() {
            final List<SessionSnapshot> sessions = List.of(high(111, 0), high(112, 1), high(113, 2), low(110, 3));
            final List<SessionKey> victims = toTest.selectVictims(snapshot(sessions, 100, 3));
            assertThat(victims).containsExactly(new SessionKey(113, 2));
        }
    }

    /// Tests for the third tier: low priority sessions filling a gap.
    @Nested
    @DisplayName("Tier Three Tests")
    class TierThreeTests {
        /// This test aims to assert that a low priority session filling a gap
        /// is evicted only when no first or second tier candidate exists, and
        /// then the highest such session goes first.
        @Test
        @DisplayName("low priority session filling a gap is evicted only as a last resort")
        void testLowFillingGapEvictedLast() {
            final SessionSnapshot incomplete = high(104, 2);
            final List<SessionSnapshot> sessions = List.of(low(102, 0), low(103, 1), incomplete);
            final List<SessionKey> victims = toTest.selectVictims(snapshot(sessions, 100, 2, incomplete));
            assertThat(victims).containsExactly(new SessionKey(103, 1));
        }
    }

    /// Tests for the state before the last verified block is known.
    @Nested
    @DisplayName("Unknown Last Verified Block Tests")
    class UnknownLastVerifiedBlockTests {
        /// This test aims to assert that while the last verified block is
        /// unknown every ordered block above zero counts as waiting, so a
        /// burst before the first success keeps the buffer bounded by
        /// evicting from the top: a low priority session at the top first.
        @Test
        @DisplayName("every block above zero waits and the top low priority session goes first")
        void testLowAtTopEvictedWhileLastVerifiedUnknown() {
            final List<SessionSnapshot> sessions = List.of(high(2, 0), high(3, 1), high(4, 2), low(5000, 3));
            final List<SessionKey> victims = toTest.selectVictims(snapshot(sessions, -1, 3));
            assertThat(victims).containsExactly(new SessionKey(5000, 3));
        }

        /// This test aims to assert that while the last verified block is
        /// unknown a burst of publisher blocks is bounded by evicting the
        /// highest complete publisher session.
        @Test
        @DisplayName("the highest publisher session goes while the last verified block is unknown")
        void testHighestPublisherEvictedWhileLastVerifiedUnknown() {
            final List<SessionSnapshot> sessions = List.of(high(2, 0), high(3, 1), high(4, 2), high(5, 3));
            final List<SessionKey> victims = toTest.selectVictims(snapshot(sessions, -1, 3));
            assertThat(victims).containsExactly(new SessionKey(5, 3));
        }
    }

    /// Tests for evicting more than one session in a single round.
    @Nested
    @DisplayName("Multi Eviction Tests")
    class MultiEvictionTests {
        /// This test aims to assert that when the buffer is over the limit by
        /// more than one, enough victims are returned to get back within the
        /// limit, highest block first, never touching the protected session.
        @Test
        @DisplayName("returns as many victims as needed, highest first")
        void testReturnsEnoughVictims() {
            final SessionSnapshot incomplete = high(106, 4);
            final List<SessionSnapshot> sessions =
                    List.of(high(102, 0), high(103, 1), high(104, 2), high(105, 3), incomplete);
            final List<SessionKey> victims = toTest.selectVictims(snapshot(sessions, 100, 3, incomplete));
            assertThat(victims).containsExactly(new SessionKey(105, 3), new SessionKey(104, 2));
        }

        /// This test aims to assert that the top of the waiting range is
        /// recomputed after every eviction: a low priority session that was
        /// filling a gap toward the evicted top becomes the new top and is
        /// evicted next, before any publisher session.
        @Test
        @DisplayName("recomputes the top of the waiting range after each eviction")
        void testRecomputesTopAfterEachEviction() {
            final List<SessionSnapshot> sessions = List.of(high(102, 0), high(103, 1), low(105, 2), low(110, 3));
            final List<SessionKey> victims = toTest.selectVictims(snapshot(sessions, 100, 2));
            assertThat(victims).containsExactly(new SessionKey(110, 3), new SessionKey(105, 2));
        }

        /// This test aims to assert that two sessions for the same block are
        /// ordered by their unique id, the newer session being evicted first.
        @Test
        @DisplayName("newer duplicate session is evicted first")
        void testNewerDuplicateEvictedFirst() {
            final List<SessionSnapshot> sessions = List.of(high(102, 0), low(110, 1), low(110, 2));
            final List<SessionKey> victims = toTest.selectVictims(snapshot(sessions, 100, 2));
            assertThat(victims).containsExactly(new SessionKey(110, 2));
        }
    }
}
