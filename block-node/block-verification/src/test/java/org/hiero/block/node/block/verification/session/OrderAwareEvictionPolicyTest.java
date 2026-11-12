// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeSet;
import org.hiero.block.node.app.fixtures.TestConfigurationBuilder;
import org.hiero.block.node.block.verification.VerificationConfig;
import org.hiero.block.node.block.verification.session.BlockVerificationSession.SessionKey;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/// Tests for the [OrderAwareEvictionPolicy], exercised on synthetic snapshots
/// without any session, executor or verification. Unless a test says otherwise,
/// the policy is built from the default configuration, whose limit of 101 puts
/// the backup position at the ninth session from the highest block down.
@DisplayName("Order Aware Eviction Policy Tests")
class OrderAwareEvictionPolicyTest {
    /// The instance under test, built from the default configuration.
    private OrderAwareEvictionPolicy toTest;

    /// Setup before each test.
    @BeforeEach
    void setUp() {
        toTest = new OrderAwareEvictionPolicy(config(Map.of()));
    }

    /// Tests for the best candidate: the stuck session with the highest block.
    @Nested
    @DisplayName("Best Candidate")
    class BestCandidate {
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

        /// This test aims to assert that a session stuck far ahead of a run is selected
        /// while the run is kept: with the last verified block 100, the sessions 101 and
        /// 102 form a run, the first gap is 103, and the session 500 is the only stuck
        /// session.
        @Test
        @DisplayName("selectForEviction() selects a session stuck far ahead of a run")
        void testSelectsASessionStuckFarAheadOfARun() {
            final NavigableSet<SessionKey> lowPriority = keys(key(101, 0), key(102, 1), key(500, 2));
            assertThat(toTest.selectForEviction(snapshot(keys(), lowPriority, null, 100L)))
                    .isEqualTo(key(500, 2));
        }

        /// This test aims to assert that the session for the next expected block is
        /// kept when a session stuck above it can go: with the last verified block 6,
        /// the low priority session 7 is the next expected block and the high
        /// priority session 9 waits above the missing block 8, so 9 is selected.
        @Test
        @DisplayName("selectForEviction() keeps the next expected block and selects a stuck session above it")
        void testKeepsTheNextExpectedBlockAndSelectsAStuckSessionAboveIt() {
            assertThat(toTest.selectForEviction(snapshot(keys(key(9, 1)), keys(key(7, 0)), null, 6L)))
                    .isEqualTo(key(9, 1));
        }

        /// This test aims to assert that a block held by sessions of both lanes keeps
        /// the walk for the first gap going: with block 7 in both lanes and block 8 in
        /// the low priority lane, the first gap is 9, so the session for block 10 is
        /// the only stuck session and it is selected.
        @Test
        @DisplayName("selectForEviction() walks past a block held in both lanes when finding the first gap")
        void testWalksPastABlockHeldInBothLanesWhenFindingTheFirstGap() {
            final NavigableSet<SessionKey> lowPriority = keys(key(7, 1), key(8, 2), key(10, 3));
            assertThat(toTest.selectForEviction(snapshot(keys(key(7, 0)), lowPriority, null, 6L)))
                    .isEqualTo(key(10, 3));
        }

        /// This test aims to assert that blocks delivered in descending order are
        /// selected highest first: with the last verified block 4, the sessions 7 to 10
        /// and then 6 to 9 wait above the missing block 5 and the highest stuck session
        /// is selected each time. Once block 5 arrives the sessions 5 to 8 form a run
        /// with the last verified block and none is stuck, and the highest of them, 8,
        /// is selected as the first backup candidate, since no high priority session is
        /// held.
        @Test
        @DisplayName(
                "selectForEviction() selects the highest session on each admission of a descending delivery, the missing block included")
        void testSelectsTheHighestSessionOnEachAdmissionOfADescendingDelivery() {
            final long lastVerified = 4L;
            final NavigableSet<SessionKey> first = keys(key(10, 0), key(9, 1), key(8, 2), key(7, 3));
            assertThat(toTest.selectForEviction(snapshot(keys(), first, null, lastVerified)))
                    .isEqualTo(key(10, 0));
            final NavigableSet<SessionKey> second = keys(key(9, 1), key(8, 2), key(7, 3), key(6, 4));
            assertThat(toTest.selectForEviction(snapshot(keys(), second, null, lastVerified)))
                    .isEqualTo(key(9, 1));
            final NavigableSet<SessionKey> third = keys(key(8, 2), key(7, 3), key(6, 4), key(5, 5));
            assertThat(toTest.selectForEviction(snapshot(keys(), third, null, lastVerified)))
                    .isEqualTo(key(8, 2));
        }

        /// This test aims to assert that with `allSourcesRequireOrdering` set to false
        /// the low priority sessions never wait for order, even far ahead of the last
        /// verified block: with the high priority session 550 above the first gap 7,
        /// the stuck high priority session is selected although the low priority
        /// session 600 is higher.
        @Test
        @DisplayName(
                "selectForEviction() never takes a low priority session as stuck when allSourcesRequireOrdering is false")
        void testNeverTakesALowPrioritySessionAsStuckWhenNotAllSourcesRequireOrdering() {
            final OrderAwareEvictionPolicy policy =
                    new OrderAwareEvictionPolicy(config(Map.of("verification.allSourcesRequireOrdering", "false")));
            final NavigableSet<SessionKey> lowPriority = keys(key(500, 0), key(600, 1));
            assertThat(policy.selectForEviction(snapshot(keys(key(550, 2)), lowPriority, null, 6L)))
                    .isEqualTo(key(550, 2));
        }
    }

    /// Tests for the sessions that complete on their own, finishing or progressing,
    /// which are never the best candidate. Each test uses the limit of 13, whose
    /// backup position is the third session from the highest block down: a backup
    /// candidate selected below the highest session shows that no session was taken
    /// as stuck, since the highest stuck session would have been selected otherwise.
    @Nested
    @DisplayName("Sessions That Complete On Their Own")
    class SessionsThatCompleteOnTheirOwn {
        /// This test aims to assert that a run contiguous with the last verified block
        /// is not taken as stuck: with the last verified block 6, the sessions 7 to 21
        /// each wait only for blocks held below them, so the third from the top, 19,
        /// is selected rather than the highest, 21.
        @Test
        @DisplayName("selectForEviction() does not take a run contiguous with the last verified block as stuck")
        void testDoesNotTakeARunContiguousWithTheLastVerifiedBlockAsStuck() {
            assertThat(policyWithLimit(13).selectForEviction(snapshot(run(7, 21, 0), keys(), null, 6L)))
                    .isEqualTo(key(19, 12));
        }

        /// This test aims to assert that sessions at or below the last verified block,
        /// such as re-deliveries, are not taken as stuck: with the last verified block
        /// 60, the sessions 40 to 54 do not wait for order, so the third from the top,
        /// 52, is selected.
        @Test
        @DisplayName("selectForEviction() does not take sessions at or below the last verified block as stuck")
        void testDoesNotTakeSessionsAtOrBelowTheLastVerifiedBlockAsStuck() {
            assertThat(policyWithLimit(13).selectForEviction(snapshot(run(40, 54, 0), keys(), null, 60L)))
                    .isEqualTo(key(52, 12));
        }

        /// This test aims to assert that sessions below `firstOrderedBlock` are not
        /// taken as stuck: with the last verified block 10, the sessions 80 to 94 sit
        /// far above the first gap, 11, yet below the first ordered block 100 they never
        /// wait for order, so the third from the top, 92, is selected.
        @Test
        @DisplayName("selectForEviction() does not take sessions below the first ordered block as stuck")
        void testDoesNotTakeSessionsBelowTheFirstOrderedBlockAsStuck() {
            final OrderAwareEvictionPolicy policy = new OrderAwareEvictionPolicy(config(
                    Map.of("verification.activeSessionsBufferSize", "13", "verification.firstOrderedBlock", "100")));
            assertThat(policy.selectForEviction(snapshot(run(80, 94, 0), keys(), null, 10L)))
                    .isEqualTo(key(92, 12));
        }

        /// This test aims to assert that before any block is verified (last verified
        /// block -1) no session is taken as stuck, since the ordering stage lets nothing
        /// wait until it adopts the first block that verifies. At the limit of 13, two
        /// snapshots are handed over: the run 1 to 13, and the sessions 0 to 2 and 5 to 14
        /// with the blocks 3 and 4 missing. Each time the third session from the top, 11
        /// and then 12, is selected as a backup candidate, where taking the sessions as
        /// stuck would select the highest, 13 and then 14.
        @Test
        @DisplayName("selectForEviction() takes no session as stuck before any block is verified")
        void testTakesNoSessionAsStuckBeforeAnyBlockIsVerified() {
            final OrderAwareEvictionPolicy policy = policyWithLimit(13);
            assertThat(policy.selectForEviction(snapshot(run(1, 13, 0), keys(), null, -1L)))
                    .isEqualTo(key(11, 10));
            final NavigableSet<SessionKey> withAGap = new TreeSet<>(run(0, 2, 0));
            withAGap.addAll(run(5, 14, 3));
            assertThat(policy.selectForEviction(snapshot(withAGap, keys(), null, -1L)))
                    .isEqualTo(key(12, 10));
        }

        /// This test aims to assert that a run spread over both lanes is not taken as
        /// stuck: with the last verified block 6, the low priority sessions 7, 8 and 10
        /// and the high priority sessions 9 and 11 to 22 hold every block from 7 to 22,
        /// so the low priority session with the highest block, 10, is selected as a
        /// backup candidate rather than the highest session, 22.
        @Test
        @DisplayName("selectForEviction() does not take a run spread over both lanes as stuck")
        void testDoesNotTakeARunSpreadOverBothLanesAsStuck() {
            final NavigableSet<SessionKey> highPriority = with(run(11, 22, 4), key(9, 3));
            final NavigableSet<SessionKey> lowPriority = keys(key(7, 0), key(8, 1), key(10, 2));
            assertThat(policyWithLimit(13).selectForEviction(snapshot(highPriority, lowPriority, null, 6L)))
                    .isEqualTo(key(10, 2));
        }
    }

    /// Tests for the first backup candidate: the highest low priority session whose
    /// eviction opens no gap below a high priority session.
    @Nested
    @DisplayName("Backup Low Priority Session Opening No Gap")
    class BackupLowPrioritySessionOpeningNoGap {
        /// This test aims to assert that when no session is stuck and no high priority
        /// session is held, the low priority session with the highest block is
        /// selected: the backfilled run 7 to 20 is contiguous with the last verified
        /// block 6, and its highest session, 20, goes.
        @Test
        @DisplayName(
                "selectForEviction() selects the highest low priority session when no high priority session is held")
        void testSelectsTheHighestLowPrioritySessionWhenNoHighPrioritySessionIsHeld() {
            assertThat(toTest.selectForEviction(snapshot(keys(), run(7, 20, 0), null, 6L)))
                    .isEqualTo(key(20, 13));
        }

        /// This test aims to assert that a low priority session at or below the last
        /// verified block goes before any high priority session, even below a
        /// publisher run: with the last verified block 100, the publisher run 101 to
        /// 113 and the historical backfill 50 and 51 are not stuck, and 51 is selected.
        @Test
        @DisplayName("selectForEviction() selects historical backfill below a publisher run")
        void testSelectsHistoricalBackfillBelowAPublisherRun() {
            final NavigableSet<SessionKey> lowPriority = keys(key(50, 13), key(51, 14));
            assertThat(toTest.selectForEviction(snapshot(run(101, 113, 0), lowPriority, null, 100L)))
                    .isEqualTo(key(51, 14));
        }

        /// This test aims to assert that a low priority session above every high
        /// priority session goes first, since no publisher block waits on it: with the
        /// last verified block 100, the publisher run 101 to 110 and the backfilled 111
        /// and 112 above it form one run, and 112 is selected.
        @Test
        @DisplayName("selectForEviction() selects a low priority session above every high priority session")
        void testSelectsALowPrioritySessionAboveEveryHighPrioritySession() {
            final NavigableSet<SessionKey> lowPriority = keys(key(111, 10), key(112, 11));
            assertThat(toTest.selectForEviction(snapshot(run(101, 110, 0), lowPriority, null, 100L)))
                    .isEqualTo(key(112, 11));
        }

        /// This test aims to assert that a low priority session whose block another
        /// session also holds goes first, since its eviction opens no gap: with the
        /// last verified block 100, the backfilled 105 is a copy of a block of the
        /// publisher run 101 to 113, and it is selected.
        @Test
        @DisplayName("selectForEviction() selects a low priority session whose block another session holds")
        void testSelectsALowPrioritySessionWhoseBlockAnotherSessionHolds() {
            assertThat(toTest.selectForEviction(snapshot(run(101, 113, 0), keys(key(105, 13)), null, 100L)))
                    .isEqualTo(key(105, 13));
        }
    }

    /// Tests for the second backup candidate: the highest low priority session, which
    /// goes before any high priority session even when high priority sessions wait on
    /// every low priority session.
    @Nested
    @DisplayName("Backup Low Priority Session")
    class BackupLowPrioritySession {
        /// This test aims to assert that a low priority session goes before any high
        /// priority session even when the whole publisher run waits on it: with the
        /// last verified block 100, the publisher holds 102 to 201 and is streaming
        /// 203, after its sessions above the missing 101 were evicted, and backfill
        /// delivers 101. Nothing other than the active session is stuck, and evicting
        /// 101 opens a gap below the run, yet 101 is selected rather than the high
        /// priority session at the backup position, the ninth from the top, 194.
        @Test
        @DisplayName("selectForEviction() selects the low priority session that closes the gap below a publisher run")
        void testSelectsTheLowPrioritySessionThatClosesTheGapBelowAPublisherRun() {
            final SessionKey active = key(203, 101);
            final NavigableSet<SessionKey> highPriority = with(run(102, 201, 1), active);
            assertThat(toTest.selectForEviction(snapshot(highPriority, keys(key(101, 0)), active, 100L)))
                    .isEqualTo(key(101, 0));
        }

        /// This test aims to assert that when high priority sessions wait on every low
        /// priority session, the one with the highest block goes: with the last
        /// verified block 100, the backfilled chunk 101 to 110 connects the publisher
        /// sessions 111 to 200 and the streaming 202, so the top of the chunk, 110, is
        /// selected rather than the ninth high priority session from the top, 193.
        @Test
        @DisplayName("selectForEviction() selects the top of a backfilled chunk below a publisher run")
        void testSelectsTheTopOfABackfilledChunkBelowAPublisherRun() {
            final SessionKey active = key(202, 100);
            final NavigableSet<SessionKey> highPriority = with(run(111, 200, 10), active);
            assertThat(toTest.selectForEviction(snapshot(highPriority, run(101, 110, 0), active, 100L)))
                    .isEqualTo(key(110, 9));
        }
    }

    /// Tests for the third backup candidate, once the low priority lane is empty: the
    /// high priority session at the backup position, counted from the highest block
    /// down.
    @Nested
    @DisplayName("Backup Position")
    class BackupPosition {
        /// This test aims to assert that the backup position is the configured limit
        /// divided by 11, truncated, and never above the third session: a limit below
        /// 33 gives the third session from the top, 33 to 43 the third, 44 the fourth,
        /// the default 101 the ninth and 127 the eleventh. Each limit is exercised with
        /// a full buffer of publisher sessions, as many as the limit, contiguous with the
        /// last verified block 1000, so that no session is stuck.
        @ParameterizedTest(name = "limit {0} selects position {1}")
        @CsvSource({"13, 3", "32, 3", "33, 3", "43, 3", "44, 4", "101, 9", "127, 11"})
        @DisplayName("selectForEviction() selects the high priority session at the backup position for the limit")
        void testSelectsTheHighPrioritySessionAtTheBackupPositionForTheLimit(final int limit, final int position) {
            final long highestBlock = 1000L + limit;
            final long expectedBlock = highestBlock - position + 1;
            final NavigableSet<SessionKey> highPriority = run(1001, highestBlock, 0);
            assertThat(policyWithLimit(limit).selectForEviction(snapshot(highPriority, keys(), null, 1000L)))
                    .isEqualTo(key(expectedBlock, expectedBlock - 1001));
        }

        /// This test aims to assert that the active publisher session above the backup
        /// position counts as one of the positions: at the limit of 13, the publisher
        /// streams 114 above the run 101 to 113, and the third session from the top,
        /// 112, is selected, not the third session other than the active one.
        @Test
        @DisplayName("selectForEviction() counts the active publisher session above the backup position")
        void testCountsTheActivePublisherSessionAboveTheBackupPosition() {
            final SessionKey active = key(114, 13);
            assertThat(policyWithLimit(13).selectForEviction(snapshot(run(101, 114, 0), keys(), active, 100L)))
                    .isEqualTo(key(112, 11));
        }

        /// This test aims to assert that when the session at the backup position is the
        /// active publisher session, the next one up is selected: at the limit of 13,
        /// the publisher resends block 112 after it sent 113 and 114, so the third
        /// session from the top is the active one, and 113, the block the publisher
        /// sends next, is selected rather than 111 below it, which would hold the
        /// active block back.
        @Test
        @DisplayName(
                "selectForEviction() selects the next session up when the backup position is the active publisher session")
        void testSelectsTheNextSessionUpWhenTheBackupPositionIsTheActivePublisherSession() {
            final SessionKey active = key(112, 13);
            final NavigableSet<SessionKey> highPriority = with(run(101, 111, 0), key(113, 11), key(114, 12), active);
            assertThat(policyWithLimit(13).selectForEviction(snapshot(highPriority, keys(), active, 100L)))
                    .isEqualTo(key(113, 11));
        }

        /// This test aims to assert that the lowest high priority session stands in for
        /// the backup position when the lane holds fewer sessions than that, so the
        /// policy selects a session for any snapshot, although a full buffer never hands
        /// over one this short: at the limit of 13, a snapshot holding only the session
        /// for block 5 and the active session 6 above it gets the lowest session, 5, the
        /// second from the top.
        @Test
        @DisplayName(
                "selectForEviction() selects the lowest high priority session when the lane is shorter than the backup position")
        void testSelectsTheLowestHighPrioritySessionWhenTheLaneIsShorterThanTheBackupPosition() {
            final SessionKey active = key(6, 1);
            assertThat(policyWithLimit(13).selectForEviction(snapshot(keys(key(5, 0), active), keys(), active, 4L)))
                    .isEqualTo(key(5, 0));
        }
    }

    /// Tests for the high priority session that is still receiving its block items.
    @Nested
    @DisplayName("Active Publisher Session")
    class ActivePublisherSession {
        /// This test aims to assert that the active publisher session is never
        /// selected, even when it is the only stuck session and the only session,
        /// because its cancellation would report an incomplete block the publisher
        /// does not resend: nothing is selected then.
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
        /// are stuck, 4 is the active publisher session, still receiving its items, and
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
        /// for the first gap: with the last verified block 6, the publisher streams the
        /// next expected block 7, the backfilled 8 and the publisher's 9 complete the
        /// run, so the first gap is 10, nothing is stuck, and the backfilled 8 is
        /// selected as a backup candidate; were block 7 not counted as held, 8 and 9
        /// would be stuck and 9 would be selected.
        @Test
        @DisplayName("selectForEviction() counts the active publisher session as holding its block")
        void testCountsTheActivePublisherSessionAsHoldingItsBlock() {
            final SessionKey active = key(7, 0);
            final NavigableSet<SessionKey> highPriority = keys(active, key(9, 2));
            assertThat(toTest.selectForEviction(snapshot(highPriority, keys(key(8, 1)), active, 6L)))
                    .isEqualTo(key(8, 1));
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

    /// Tests for the guarantee that the policy always selects a session it may give up.
    @Nested
    @DisplayName("Always Selects")
    class AlwaysSelects {
        /// This test aims to assert that nothing is selected only when the snapshot
        /// holds no session but the active publisher session: an empty snapshot and a
        /// snapshot of the active session alone yield nothing, while a single other
        /// session, here a backfilled block at or below the last verified block, is
        /// selected.
        @Test
        @DisplayName(
                "selectForEviction() selects nothing only when the snapshot holds no session but the active publisher session")
        void testSelectsNothingOnlyWhenTheSnapshotHoldsNoSessionButTheActiveOne() {
            final SessionKey active = key(11, 0);
            assertThat(toTest.selectForEviction(snapshot(keys(), keys(), null, 6L)))
                    .isNull();
            assertThat(toTest.selectForEviction(snapshot(keys(active), keys(), active, 6L)))
                    .isNull();
            assertThat(toTest.selectForEviction(snapshot(keys(active), keys(key(5, 1)), active, 6L)))
                    .isEqualTo(key(5, 1));
        }

        /// This test aims to assert that whatever the snapshot, the policy selects a
        /// session whenever the snapshot holds one other than the active publisher
        /// session, never the active one, and only a key the snapshot holds: two
        /// hundred snapshots of random lanes, last verified blocks and active sessions,
        /// drawn from a fixed seed so every run is the same, are each handed to the
        /// policy at the default limit and at the minimum limit of 13.
        @Test
        @DisplayName(
                "selectForEviction() selects a held session other than the active one whenever the snapshot holds one")
        void testSelectsAHeldSessionOtherThanTheActiveOneWheneverTheSnapshotHoldsOne() {
            final Random random = new Random(3634L);
            final List<OrderAwareEvictionPolicy> policies = List.of(toTest, policyWithLimit(13));
            for (int round = 0; round < 200; round++) {
                final long lastVerified = random.nextInt(10) - 1L;
                final NavigableSet<SessionKey> highPriority = randomLane(random, lastVerified, 0);
                final NavigableSet<SessionKey> lowPriority = randomLane(random, lastVerified, 1000);
                final SessionKey active;
                if (highPriority.isEmpty() || random.nextBoolean()) {
                    active = null;
                } else {
                    active = List.copyOf(highPriority).get(random.nextInt(highPriority.size()));
                }
                final ActiveSessionsSnapshot snapshot = snapshot(highPriority, lowPriority, active, lastVerified);
                final int otherSessions = active == null ? snapshot.size() : snapshot.size() - 1;
                for (final OrderAwareEvictionPolicy policy : policies) {
                    final SessionKey selected = policy.selectForEviction(snapshot);
                    if (otherSessions > 0) {
                        assertThat(selected).isNotNull().isNotEqualTo(active);
                        assertThat(highPriority.contains(selected) || lowPriority.contains(selected))
                                .isTrue();
                    } else {
                        assertThat(selected).isNull();
                    }
                }
            }
        }

        /// A lane of random keys: up to twenty sessions for blocks from three below to
        /// twenty five above the last verified block, never below block 0, with unique ids
        /// counting up from the given first id.
        private static NavigableSet<SessionKey> randomLane(
                final Random random, final long lastVerifiedBlock, final long firstUniqueId) {
            final NavigableSet<SessionKey> lane = new TreeSet<>();
            final int size = random.nextInt(21);
            for (int index = 0; index < size; index++) {
                final long blockNumber = Math.max(0L, lastVerifiedBlock - 3 + random.nextInt(29));
                lane.add(key(blockNumber, firstUniqueId + index));
            }
            return lane;
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

    /// The policy built from the default configuration with the given limit.
    private static OrderAwareEvictionPolicy policyWithLimit(final int limit) {
        return new OrderAwareEvictionPolicy(
                config(Map.of("verification.activeSessionsBufferSize", String.valueOf(limit))));
    }

    /// A session key.
    private static SessionKey key(final long blockNumber, final long uniqueId) {
        return new SessionKey(blockNumber, uniqueId);
    }

    /// An ascending set of keys.
    private static NavigableSet<SessionKey> keys(final SessionKey... keys) {
        return new TreeSet<>(List.of(keys));
    }

    /// The keys of a run of sessions, one for each block from the first block to the
    /// last, with unique ids counting up from the given first id.
    private static NavigableSet<SessionKey> run(final long firstBlock, final long lastBlock, final long firstUniqueId) {
        final NavigableSet<SessionKey> run = new TreeSet<>();
        for (long blockNumber = firstBlock; blockNumber <= lastBlock; blockNumber++) {
            run.add(key(blockNumber, firstUniqueId + blockNumber - firstBlock));
        }
        return run;
    }

    /// An ascending set of the given keys and more keys.
    private static NavigableSet<SessionKey> with(final NavigableSet<SessionKey> keys, final SessionKey... more) {
        final NavigableSet<SessionKey> result = new TreeSet<>(keys);
        result.addAll(List.of(more));
        return result;
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
