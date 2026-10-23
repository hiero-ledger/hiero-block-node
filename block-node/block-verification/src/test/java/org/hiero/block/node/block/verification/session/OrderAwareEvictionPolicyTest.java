// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import org.hiero.block.node.app.fixtures.TestConfigurationBuilder;
import org.hiero.block.node.block.verification.VerificationConfig;
import org.hiero.block.node.block.verification.session.BlockVerificationSession.SessionKey;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/// Tests for the [OrderAwareEvictionPolicy], exercised on synthetic snapshots
/// without any session, executor or verification.
@DisplayName("Order Aware Eviction Policy Tests")
class OrderAwareEvictionPolicyTest {
    /// The instance under test, built from the default configuration.
    private OrderAwareEvictionPolicy toTest;

    /// Setup before each test.
    @BeforeEach
    void setUp() {
        toTest = new OrderAwareEvictionPolicy(config(Map.of()));
    }

    /// Tests for the sessions that complete on their own, finishing or progressing,
    /// which the policy never selects.
    @Nested
    @DisplayName("Sessions That Complete On Their Own")
    class SessionsThatCompleteOnTheirOwn {
        /// This test aims to assert that for a snapshot without any session nothing is
        /// selected, since there is nothing to give up.
        @Test
        @DisplayName("selectForEviction() returns nothing for empty lanes")
        void testSelectsNothingWhenBothLanesAreEmpty() {
            assertThat(toTest.selectForEviction(snapshot(keys(), keys(), null, 6L)))
                    .isNull();
        }

        /// This test aims to assert that sessions for blocks at or below the last
        /// verified block, such as historical backfill or a re-delivery, are never
        /// selected: they do not wait for order and complete on their own. With the
        /// last verified block 50, the low priority sessions 45, 48 and 50 are not
        /// selected, alone or together with the high priority session for the next
        /// expected block 51, which is finishing as well.
        @Test
        @DisplayName("selectForEviction() never selects a session at or below the last verified block")
        void testNeverSelectsSessionsAtOrBelowTheLastVerifiedBlock() {
            final NavigableSet<SessionKey> lowPriority = keys(key(45, 0), key(48, 1), key(50, 2));
            assertThat(toTest.selectForEviction(snapshot(keys(), lowPriority, null, 50L)))
                    .isNull();
            assertThat(toTest.selectForEviction(snapshot(keys(key(51, 3)), lowPriority, null, 50L)))
                    .isNull();
        }

        /// This test aims to assert that a session for the next expected block is
        /// never selected: alone it is finishing and nothing is selected, and with a
        /// session for block 9 above the missing block 8 the stuck session 9 is
        /// selected while the next expected block 7 is kept.
        @Test
        @DisplayName("selectForEviction() keeps the next expected block and selects a stuck session above it")
        void testNeverSelectsTheNextExpectedBlock() {
            final NavigableSet<SessionKey> lowPriority = keys(key(7, 0));
            assertThat(toTest.selectForEviction(snapshot(keys(), lowPriority, null, 6L)))
                    .isNull();
            assertThat(toTest.selectForEviction(snapshot(keys(key(9, 1)), lowPriority, null, 6L)))
                    .isEqualTo(key(9, 1));
        }

        /// This test aims to assert that a run contiguous with the last verified block
        /// is never selected, even when its blocks are spread over both lanes: every
        /// block from 7 to 12 is held by a session of one of the two lanes, so the first
        /// gap is 13, every session is progressing or finishing, and nothing is
        /// selected.
        @Test
        @DisplayName(
                "selectForEviction() never selects a run contiguous with the last verified block across both lanes")
        void testNeverSelectsARunContiguousWithTheLastVerifiedBlock() {
            final NavigableSet<SessionKey> highPriority = keys(key(9, 2), key(11, 4), key(12, 5));
            final NavigableSet<SessionKey> lowPriority = keys(key(7, 0), key(8, 1), key(10, 3));
            assertThat(toTest.selectForEviction(snapshot(highPriority, lowPriority, null, 6L)))
                    .isNull();
        }

        /// This test aims to assert that sessions below `firstOrderedBlock` never wait
        /// for order and are never selected: before any block is verified the first gap
        /// is block 0, yet the sessions 95, 96 and 97 below the first ordered block 100
        /// are not selected, while the ordered session 100 above the gap is stuck and
        /// selected.
        @Test
        @DisplayName("selectForEviction() never selects a session below the first ordered block")
        void testNeverSelectsSessionsBelowTheFirstOrderedBlock() {
            final OrderAwareEvictionPolicy policy =
                    new OrderAwareEvictionPolicy(config(Map.of("verification.firstOrderedBlock", "100")));
            final NavigableSet<SessionKey> lowPriority = keys(key(95, 0), key(96, 1), key(97, 2));
            assertThat(policy.selectForEviction(snapshot(keys(), lowPriority, null, -1L)))
                    .isNull();
            assertThat(policy.selectForEviction(snapshot(keys(key(100, 3)), lowPriority, null, -1L)))
                    .isEqualTo(key(100, 3));
        }

        /// This test aims to assert that with `allSourcesRequireOrdering` set to false
        /// the low priority sessions never wait for order and are never selected, even
        /// far ahead of the last verified block: the backfilled sessions 500 and 600
        /// alone yield nothing, and with the high priority session 550 above the first
        /// gap 7, the high priority session is selected although 600 is higher.
        @Test
        @DisplayName("selectForEviction() never selects a low priority session when allSourcesRequireOrdering is false")
        void testNeverSelectsLowPrioritySessionsWhenNotAllSourcesRequireOrdering() {
            final OrderAwareEvictionPolicy policy =
                    new OrderAwareEvictionPolicy(config(Map.of("verification.allSourcesRequireOrdering", "false")));
            final NavigableSet<SessionKey> lowPriority = keys(key(500, 0), key(600, 1));
            assertThat(policy.selectForEviction(snapshot(keys(), lowPriority, null, 6L)))
                    .isNull();
            assertThat(policy.selectForEviction(snapshot(keys(key(550, 2)), lowPriority, null, 6L)))
                    .isEqualTo(key(550, 2));
        }
    }

    /// Tests for finding the first gap, the lowest block after the last verified
    /// block that no session holds.
    @Nested
    @DisplayName("First Gap")
    class FirstGap {
        /// This test aims to assert that a block held by sessions of both lanes is
        /// counted once when the first gap is found: with block 7 in both lanes and
        /// block 8 in the low priority lane, the first gap is 9 and nothing is stuck;
        /// adding a session for block 10 makes it the only stuck session, and it is
        /// selected.
        @Test
        @DisplayName("selectForEviction() counts a block held in both lanes once when finding the first gap")
        void testCountsABlockHeldInBothLanesOnce() {
            final NavigableSet<SessionKey> highPriority = keys(key(7, 0));
            assertThat(toTest.selectForEviction(snapshot(highPriority, keys(key(7, 1), key(8, 2)), null, 6L)))
                    .isNull();
            assertThat(toTest.selectForEviction(
                            snapshot(highPriority, keys(key(7, 1), key(8, 2), key(10, 3)), null, 6L)))
                    .isEqualTo(key(10, 3));
        }

        /// This test aims to assert that before any block is verified (last verified
        /// block -1) block 0 is the next expected block: a run starting at block 0 is
        /// not stuck and nothing is selected, while without block 0 every session is
        /// stuck and the highest is selected.
        @Test
        @DisplayName("selectForEviction() expects block 0 next before any block is verified")
        void testStartsTheGapSearchAtBlockZeroBeforeAnyBlockIsVerified() {
            assertThat(toTest.selectForEviction(snapshot(keys(key(0, 0), key(1, 1), key(2, 2)), keys(), null, -1L)))
                    .isNull();
            assertThat(toTest.selectForEviction(snapshot(keys(key(1, 0), key(2, 1), key(3, 2)), keys(), null, -1L)))
                    .isEqualTo(key(3, 2));
        }

        /// This test aims to assert that when blocks arrive in descending order, each
        /// admission selects the highest stuck session while the lowest block is
        /// missing, and that nothing is selected once it arrives: with the last
        /// verified block 4, sessions 7 to 10 and then 6 to 9 wait above the missing
        /// block 5 and the highest is selected each time, while sessions 5 to 8 form a
        /// run with the last verified block and none is stuck.
        @Test
        @DisplayName(
                "selectForEviction() selects the highest stuck session on each admission of a descending delivery until the missing block arrives")
        void testStopsSelectingOnceTheMissingBlockOfADescendingDeliveryArrives() {
            final long lastVerified = 4L;
            final NavigableSet<SessionKey> first = keys(key(10, 0), key(9, 1), key(8, 2), key(7, 3));
            assertThat(toTest.selectForEviction(snapshot(keys(), first, null, lastVerified)))
                    .isEqualTo(key(10, 0));
            final NavigableSet<SessionKey> second = keys(key(9, 1), key(8, 2), key(7, 3), key(6, 4));
            assertThat(toTest.selectForEviction(snapshot(keys(), second, null, lastVerified)))
                    .isEqualTo(key(9, 1));
            final NavigableSet<SessionKey> third = keys(key(8, 2), key(7, 3), key(6, 4), key(5, 5));
            assertThat(toTest.selectForEviction(snapshot(keys(), third, null, lastVerified)))
                    .isNull();
        }
    }

    /// Tests for the choice among the stuck sessions.
    @Nested
    @DisplayName("Stuck Session Selection")
    class StuckSessionSelection {
        /// This test aims to assert that among stuck sessions the one with the highest
        /// block is selected: with the last verified block 6 and block 7 missing, the
        /// sessions 8, 9, 10 and 16 are all stuck, and 16, which no session waits for,
        /// is selected.
        @Test
        @DisplayName("selectForEviction() selects the highest stuck session")
        void testSelectsTheHighestStuckSession() {
            final NavigableSet<SessionKey> lowPriority = keys(key(8, 0), key(9, 1), key(10, 2), key(16, 3));
            assertThat(toTest.selectForEviction(snapshot(keys(), lowPriority, null, 6L)))
                    .isEqualTo(key(16, 3));
        }

        /// This test aims to assert that the highest stuck session is selected whatever
        /// its lane: above the missing block 7, the complete high priority session 10 is
        /// selected over the low priority sessions 8 and 9 below it, and the low
        /// priority session 20 is selected over the high priority session 10 below it.
        @Test
        @DisplayName("selectForEviction() selects the highest stuck session whatever its lane")
        void testSelectsTheHighestStuckSessionWhateverItsLane() {
            final NavigableSet<SessionKey> highPriority = keys(key(10, 0));
            assertThat(toTest.selectForEviction(snapshot(highPriority, keys(key(8, 1), key(9, 2)), null, 6L)))
                    .isEqualTo(key(10, 0));
            assertThat(toTest.selectForEviction(snapshot(highPriority, keys(key(8, 1), key(20, 2)), null, 6L)))
                    .isEqualTo(key(20, 2));
        }

        /// This test aims to assert that when sessions of both lanes hold the highest
        /// stuck block, the low priority one is selected and the publisher's copy is
        /// kept, and that the lane decides, not the unique id: the low priority session
        /// goes whether it is the newer or the older of the two.
        @Test
        @DisplayName(
                "selectForEviction() selects the low priority session when both lanes hold the highest stuck block")
        void testSelectsTheLowPrioritySessionOnATie() {
            assertThat(toTest.selectForEviction(snapshot(keys(key(10, 0)), keys(key(10, 1)), null, 6L)))
                    .isEqualTo(key(10, 1));
            assertThat(toTest.selectForEviction(snapshot(keys(key(10, 5)), keys(key(10, 2)), null, 6L)))
                    .isEqualTo(key(10, 2));
        }

        /// This test aims to assert that when one lane holds two sessions for the
        /// highest stuck block, the one with the higher unique id (the newer) is
        /// selected, keeping the older session that is further along.
        @Test
        @DisplayName("selectForEviction() selects the newer of two sessions for the highest stuck block in one lane")
        void testSelectsTheNewerOfTwoSessionsForTheSameBlockInALane() {
            final NavigableSet<SessionKey> lowPriority = keys(key(55, 0), key(56, 1), key(57, 2), key(57, 5));
            assertThat(toTest.selectForEviction(snapshot(keys(), lowPriority, null, 50L)))
                    .isEqualTo(key(57, 5));
        }

        /// This test aims to assert that a session far ahead of a run, such as a
        /// newcomer just admitted, is selected while the run is kept: with the last
        /// verified block 100, the sessions 101 and 102 form a run, the first gap is
        /// 103, and the session 500 is the only stuck session.
        @Test
        @DisplayName("selectForEviction() selects a session stuck far ahead of a run, the newcomer included")
        void testSelectsAStuckNewcomerFarAheadOfARun() {
            final NavigableSet<SessionKey> lowPriority = keys(key(101, 0), key(102, 1), key(500, 2));
            assertThat(toTest.selectForEviction(snapshot(keys(), lowPriority, null, 100L)))
                    .isEqualTo(key(500, 2));
        }
    }

    /// Tests for the high priority session that is still receiving its block items.
    @Nested
    @DisplayName("Active Publisher Session")
    class ActivePublisherSession {
        /// This test aims to assert that the active publisher session is never
        /// selected, even when it is the only stuck session, because its cancellation
        /// would report an incomplete block the publisher does not resend.
        @Test
        @DisplayName(
                "selectForEviction() never selects the active publisher session, even when it is the only stuck session")
        void testNeverSelectsTheActivePublisherSession() {
            final SessionKey active = key(11, 0);
            assertThat(toTest.selectForEviction(snapshot(keys(active), keys(), active, 6L)))
                    .isNull();
        }

        /// This test aims to assert that in a publisher only burst behind a missing
        /// block, the highest stuck session other than the active one is selected:
        /// with the last verified block 0 and block 1 missing, the sessions 2, 3 and 4
        /// are stuck, 4 is the active publisher session during its own admission, and
        /// 3 is selected.
        @Test
        @DisplayName(
                "selectForEviction() selects the highest stuck session below the active publisher session in a burst")
        void testSelectsTheHighestStuckSessionBelowTheActiveOneInABurst() {
            final SessionKey active = key(4, 2);
            final NavigableSet<SessionKey> highPriority = keys(key(2, 0), key(3, 1), active);
            assertThat(toTest.selectForEviction(snapshot(highPriority, keys(), active, 0L)))
                    .isEqualTo(key(3, 1));
        }

        /// This test aims to assert that the active publisher session is skipped
        /// wherever it sits: after a resend it can sit below complete sessions, and
        /// the highest stuck session above it is still the one selected.
        @Test
        @DisplayName("selectForEviction() skips the active publisher session wherever it sits")
        void testSkipsTheActivePublisherSessionWhereverItSits() {
            final SessionKey active = key(8, 0);
            final NavigableSet<SessionKey> highPriority = keys(active, key(9, 1), key(10, 2));
            assertThat(toTest.selectForEviction(snapshot(highPriority, keys(), active, 6L)))
                    .isEqualTo(key(10, 2));
        }

        /// This test aims to assert that the active publisher session holds its block
        /// for the first gap: while the publisher streams the next expected block 7,
        /// the backfilled sessions 8 and 9 form a run with it, the first gap is 10,
        /// and nothing is selected.
        @Test
        @DisplayName("selectForEviction() counts the active publisher session as holding its block")
        void testCountsTheActivePublisherSessionAsHoldingItsBlock() {
            final SessionKey active = key(7, 0);
            assertThat(toTest.selectForEviction(snapshot(keys(active), keys(key(8, 1), key(9, 2)), active, 6L)))
                    .isNull();
        }

        /// This test aims to assert that a stuck low priority session above the active
        /// publisher session is selected: with block 7 missing, the active session 8 and
        /// the backfilled session 9 are both stuck, and the backfilled one goes.
        @Test
        @DisplayName("selectForEviction() selects a stuck low priority session above the active publisher session")
        void testSelectsAStuckLowPrioritySessionAboveTheActiveOne() {
            final SessionKey active = key(8, 0);
            assertThat(toTest.selectForEviction(snapshot(keys(active), keys(key(9, 1)), active, 6L)))
                    .isEqualTo(key(9, 1));
        }
    }

    /// Tests for the validation of the policy's input.
    @Nested
    @DisplayName("Input Contract")
    class InputContract {
        /// This test aims to assert that the policy validates its input and rejects a
        /// null snapshot with a `NullPointerException`.
        @Test
        @DisplayName("selectForEviction() rejects a null snapshot")
        void testRejectsANullSnapshot() {
            assertThatNullPointerException().isThrownBy(this::selectWithNullSnapshot);
        }

        /// Calls the policy with a null snapshot.
        private void selectWithNullSnapshot() {
            toTest.selectForEviction(null);
        }
    }

    /// Build a verification configuration with the given overrides on top of the defaults.
    private static VerificationConfig config(final Map<String, String> overrides) {
        TestConfigurationBuilder builder = new TestConfigurationBuilder().withConfigDataType(VerificationConfig.class);
        for (final Map.Entry<String, String> override : overrides.entrySet()) {
            builder = builder.withValue(override.getKey(), override.getValue());
        }
        return builder.getOrCreateConfig().getConfigData(VerificationConfig.class);
    }

    /// A session key.
    private static SessionKey key(final long blockNumber, final long uniqueId) {
        return new SessionKey(blockNumber, uniqueId);
    }

    /// An ascending set of keys.
    private static NavigableSet<SessionKey> keys(final SessionKey... keys) {
        return new TreeSet<>(List.of(keys));
    }

    /// A snapshot.
    private static ActiveSessionsSnapshot snapshot(
            final NavigableSet<SessionKey> highPriority,
            final NavigableSet<SessionKey> lowPriority,
            final SessionKey activeHighPrioritySession,
            final long lastVerifiedBlock) {
        return new ActiveSessionsSnapshot(highPriority, lowPriority, activeHighPrioritySession, lastVerifiedBlock);
    }
}
