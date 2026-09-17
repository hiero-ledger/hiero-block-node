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
/// last verified block, the limit and the protected keys, and asserts exactly
/// which keys come back, in which order.
@DisplayName("Gap Aware Eviction Policy Tests")
class GapAwareEvictionPolicyTest {
    /// The policy under test, stateless.
    private final GapAwareEvictionPolicy toTest = new GapAwareEvictionPolicy();

    /// Build a high priority publisher session snapshot with a complete block.
    private static SessionSnapshot high(final long blockNumber, final long uniqueId) {
        return new SessionSnapshot(
                new SessionKey(blockNumber, uniqueId), SessionPriority.HIGH, BlockSource.PUBLISHER, true);
    }

    /// Build a low priority backfill session snapshot with a complete block.
    private static SessionSnapshot low(final long blockNumber, final long uniqueId) {
        return new SessionSnapshot(
                new SessionKey(blockNumber, uniqueId), SessionPriority.LOW, BlockSource.BACKFILL, true);
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
        /// buffer is at or below its limit, even when every session is stuck
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
    @DisplayName("Non Stuck Sessions Are Never Evicted")
    class NonStuckSessionTests {
        /// This test aims to assert that low priority sessions for blocks below
        /// the next expected block (historical backfill) are never evicted, and
        /// that the highest stuck publisher session goes instead, even though
        /// low priority sessions are normally evicted first.
        @Test
        @DisplayName("historical backfill sessions below the next expected block are kept")
        void testHistoricalBackfillKept() {
            final List<SessionSnapshot> sessions = List.of(low(5, 0), low(6, 1), low(7, 2), high(102, 3), high(103, 4));
            final List<SessionKey> victims = toTest.selectVictims(snapshot(sessions, 100, 4, low(7, 2)));
            assertThat(victims).containsExactly(new SessionKey(103, 4));
        }

        /// This test aims to assert that publisher sessions at or below the
        /// last verified block (duplicates) and the session for the next
        /// expected block (the head of the chain) are never evicted, and that
        /// the highest stuck publisher session is chosen instead.
        @Test
        @DisplayName("duplicates and the chain head are kept")
        void testDuplicatesAndHeadKept() {
            final List<SessionSnapshot> sessions = List.of(high(100, 0), high(101, 1), high(102, 2), high(103, 3));
            final List<SessionKey> victims = toTest.selectVictims(snapshot(sessions, 100, 3));
            assertThat(victims).containsExactly(new SessionKey(103, 3));
        }

        /// This test aims to assert that when sources other than the publisher
        /// are not subject to ordering, low priority sessions far ahead are not
        /// stuck and are therefore kept, while a stuck publisher session is evicted.
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
        @DisplayName("returns nothing when only non stuck sessions are over the limit")
        void testOvershootWithOnlyNonStuckSessions() {
            final List<SessionSnapshot> sessions = List.of(low(5, 0), low(6, 1), low(7, 2));
            assertThat(toTest.selectVictims(snapshot(sessions, 100, 2))).isEmpty();
        }
    }

    /// Tests for the first tier: stuck low priority sessions not filling a gap.
    @Nested
    @DisplayName("Tier One Tests")
    class TierOneTests {
        /// This test aims to assert that a stuck low priority session at the
        /// top of the chain, which no other session depends on, is evicted
        /// before any publisher session.
        @Test
        @DisplayName("stuck low priority top of chain is evicted before publisher sessions")
        void testStuckLowTopEvictedFirst() {
            final List<SessionSnapshot> sessions = List.of(high(102, 0), high(103, 1), high(104, 2), low(110, 3));
            final List<SessionKey> victims = toTest.selectVictims(snapshot(sessions, 100, 3));
            assertThat(victims).containsExactly(new SessionKey(110, 3));
        }

        /// This test aims to assert that a low priority session filling a gap
        /// that a higher publisher session waits for is not evicted while a
        /// publisher session can be evicted instead.
        @Test
        @DisplayName("low priority session filling a gap is kept while a publisher alternative exists")
        void testNeededLowKept() {
            final List<SessionSnapshot> sessions = List.of(low(102, 0), high(103, 1), high(104, 2), high(105, 3));
            final List<SessionKey> victims = toTest.selectVictims(snapshot(sessions, 100, 3));
            assertThat(victims).containsExactly(new SessionKey(105, 3));
        }

        /// This test aims to assert that a low priority session so far ahead
        /// that it cannot be released before the buffer turns over is evicted
        /// even when it is the protected, just activated session, so junk from
        /// a bad peer never forces a publisher eviction.
        @Test
        @DisplayName("far ahead low priority session evicts itself even when protected")
        void testFarAheadLowEvictsItself() {
            final SessionSnapshot junk = low(5000, 3);
            final List<SessionSnapshot> sessions = List.of(high(102, 0), high(103, 1), high(104, 2), junk);
            final List<SessionKey> victims = toTest.selectVictims(snapshot(sessions, 100, 3, junk));
            assertThat(victims).containsExactly(junk.key());
        }

        /// This test aims to assert that the far ahead rule is disabled while
        /// the last verified block is unknown, so a node starting mid chain can
        /// seed the last verified block with its first success; the protected
        /// session is then kept and the highest publisher session goes.
        @Test
        @DisplayName("far ahead rule is disabled while the last verified block is unknown")
        void testFarAheadRuleDisabledAtStartup() {
            final SessionSnapshot current = low(5000, 3);
            final List<SessionSnapshot> sessions = List.of(high(2, 0), high(3, 1), high(4, 2), current);
            final List<SessionKey> victims = toTest.selectVictims(snapshot(sessions, -1, 3, current));
            assertThat(victims).containsExactly(new SessionKey(4, 2));
        }
    }

    /// Tests for the second tier: stuck publisher sessions.
    @Nested
    @DisplayName("Tier Two Tests")
    class TierTwoTests {
        /// This test aims to assert that among publisher sessions the highest
        /// non protected block is evicted, keeping the base of the chain intact
        /// so it releases the moment the missing block arrives.
        @Test
        @DisplayName("highest non protected publisher session is evicted")
        void testHighestNonProtectedPublisherEvicted() {
            final SessionSnapshot current = high(105, 3);
            final List<SessionSnapshot> sessions = List.of(high(102, 0), high(103, 1), high(104, 2), current);
            final List<SessionKey> victims = toTest.selectVictims(snapshot(sessions, 100, 3, current));
            assertThat(victims).containsExactly(new SessionKey(104, 2));
        }

        /// This test aims to assert that when every stuck session is protected
        /// the policy returns no victims instead of violating the protection.
        @Test
        @DisplayName("returns nothing when every stuck session is protected")
        void testAllProtected() {
            final SessionSnapshot first = high(102, 0);
            final SessionSnapshot second = high(103, 1);
            final List<SessionKey> victims =
                    toTest.selectVictims(snapshot(List.of(first, second), 100, 1, first, second));
            assertThat(victims).isEmpty();
        }

        /// This test aims to assert the reverse order scenario from the design
        /// call: blocks 10, 9, 8 fill a buffer of three and block 7 arrives as
        /// the protected current session; the highest block is evicted and the
        /// buffer never grows past its limit.
        @Test
        @DisplayName("reverse order keeps the newest lowest block and evicts the highest")
        void testReverseOrder() {
            final SessionSnapshot current = high(7, 3);
            final List<SessionSnapshot> sessions = List.of(high(10, 0), high(9, 1), high(8, 2), current);
            final List<SessionKey> victims = toTest.selectVictims(snapshot(sessions, 5, 3, current));
            assertThat(victims).containsExactly(new SessionKey(10, 0));
        }

        /// This test aims to assert that a low priority block arriving in
        /// reverse order inside the gap a parked publisher chain waits for is
        /// kept, and the top of the publisher chain is evicted instead.
        @Test
        @DisplayName("reverse order backfill inside the needed range is kept")
        void testReverseOrderBackfillInsideGapKept() {
            final SessionSnapshot current = low(110, 3);
            final List<SessionSnapshot> sessions = List.of(high(111, 0), high(112, 1), high(113, 2), current);
            final List<SessionKey> victims = toTest.selectVictims(snapshot(sessions, 100, 3, current));
            assertThat(victims).containsExactly(new SessionKey(113, 2));
        }
    }

    /// Tests for the third tier: stuck low priority sessions filling a gap.
    @Nested
    @DisplayName("Tier Three Tests")
    class TierThreeTests {
        /// This test aims to assert that a low priority session filling a gap
        /// is evicted only when no first or second tier candidate exists, and
        /// then the highest such session goes first.
        @Test
        @DisplayName("needed low priority session is evicted only as a last resort")
        void testNeededLowEvictedLast() {
            final SessionSnapshot current = high(104, 2);
            final List<SessionSnapshot> sessions = List.of(low(102, 0), low(103, 1), current);
            final List<SessionKey> victims = toTest.selectVictims(snapshot(sessions, 100, 2, current));
            assertThat(victims).containsExactly(new SessionKey(103, 1));
        }
    }

    /// Tests for evicting more than one session in a single round.
    @Nested
    @DisplayName("Multi Eviction Tests")
    class MultiEvictionTests {
        /// This test aims to assert that when the buffer is over the limit by
        /// more than one, enough victims are returned to get back within the
        /// limit, highest block first.
        @Test
        @DisplayName("returns as many victims as needed, highest first")
        void testReturnsEnoughVictims() {
            final SessionSnapshot current = high(106, 4);
            final List<SessionSnapshot> sessions =
                    List.of(high(102, 0), high(103, 1), high(104, 2), high(105, 3), current);
            final List<SessionKey> victims = toTest.selectVictims(snapshot(sessions, 100, 3, current));
            assertThat(victims).containsExactly(new SessionKey(105, 3), new SessionKey(104, 2));
        }

        /// This test aims to assert that the highest stuck block is recomputed
        /// after every eviction: a low priority session that was filling a gap
        /// toward the evicted top becomes the new top and is evicted next,
        /// before any publisher session.
        @Test
        @DisplayName("recomputes the top of the chain after each eviction")
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
