// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.subcommands;

import static org.hiero.block.tools.days.subcommands.LiveSequential.CACHE_SETTLE_BUFFER;
import static org.hiero.block.tools.days.subcommands.LiveSequential.isCacheSafeForDay;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for {@link LiveSequential#isCacheSafeForDay(LocalDate, Instant)}, the recency
 * gate that prevents the end-of-day cache-stall described in issue #3716.
 *
 * <p>The bug was that the "Insufficient signatures for block N" retry loop treated the on-disk
 * GCPBucketLister list-cache as authoritative for any day that was not "today", so tail-of-day
 * signature files that landed on GCS after the cache was first written were never picked up. These
 * tests pin down the boundary conditions of the settle-buffer gate that fixes it.
 */
@DisplayName("LiveSequential.isCacheSafeForDay")
class LiveSequentialCacheSafetyTest {

    /** UTC midnight of an arbitrary reference day used as "the start of blockDay + 1". */
    private static final LocalDate BLOCK_DAY = LocalDate.of(2026, 9, 28);
    /** End of {@link #BLOCK_DAY}, i.e. UTC midnight opening 2026-09-29. */
    private static final Instant END_OF_BLOCK_DAY =
            BLOCK_DAY.plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant();

    @Nested
    @DisplayName("returns false — cache must be bypassed")
    class Unsafe {

        @Test
        @DisplayName("day is still in progress (now = mid-day of blockDay)")
        void midDayOfBlockDay() {
            Instant now = BLOCK_DAY.atTime(12, 0).toInstant(ZoneOffset.UTC);
            assertFalse(isCacheSafeForDay(BLOCK_DAY, now));
        }

        @Test
        @DisplayName("day just ended (now = end of blockDay)")
        void exactlyEndOfBlockDay() {
            assertFalse(isCacheSafeForDay(BLOCK_DAY, END_OF_BLOCK_DAY));
        }

        @Test
        @DisplayName("day ended 1 minute ago")
        void oneMinuteAfterBlockDay() {
            Instant now = END_OF_BLOCK_DAY.plus(Duration.ofMinutes(1));
            assertFalse(isCacheSafeForDay(BLOCK_DAY, now));
        }

        @Test
        @DisplayName("day ended just under the settle buffer (buffer - 1 second)")
        void justUnderSettleBuffer() {
            Instant now = END_OF_BLOCK_DAY.plus(CACHE_SETTLE_BUFFER.minusSeconds(1));
            assertFalse(isCacheSafeForDay(BLOCK_DAY, now));
        }
    }

    @Nested
    @DisplayName("returns true — cache is safe to reuse")
    class Safe {

        @Test
        @DisplayName("exactly at the settle-buffer boundary")
        void exactlyAtSettleBuffer() {
            Instant now = END_OF_BLOCK_DAY.plus(CACHE_SETTLE_BUFFER);
            assertTrue(isCacheSafeForDay(BLOCK_DAY, now));
        }

        @Test
        @DisplayName("one second past the settle buffer")
        void oneSecondPastSettleBuffer() {
            Instant now = END_OF_BLOCK_DAY.plus(CACHE_SETTLE_BUFFER.plusSeconds(1));
            assertTrue(isCacheSafeForDay(BLOCK_DAY, now));
        }

        @Test
        @DisplayName("day ended many days ago")
        void wellPastBlockDay() {
            Instant now = END_OF_BLOCK_DAY.plus(Duration.ofDays(30));
            assertTrue(isCacheSafeForDay(BLOCK_DAY, now));
        }

        @Test
        @DisplayName("day ended many months ago")
        void manyMonthsPastBlockDay() {
            Instant now = END_OF_BLOCK_DAY.plus(Duration.ofDays(365));
            assertTrue(isCacheSafeForDay(BLOCK_DAY, now));
        }
    }

    @Test
    @DisplayName("settle buffer defaults to 30 minutes")
    void settleBufferDefault() {
        assertEquals(Duration.ofMinutes(30), CACHE_SETTLE_BUFFER);
    }

    @Test
    @DisplayName("regression: recovers automatically once the settle buffer elapses")
    void recoversAfterSettleBuffer() {
        Instant justAfterMidnight = END_OF_BLOCK_DAY.plus(Duration.ofSeconds(1));
        Instant justAfterBuffer = END_OF_BLOCK_DAY.plus(CACHE_SETTLE_BUFFER.plusSeconds(1));

        // In the buggy pre-fix behaviour, the retry loop would loop forever at the first instant
        // reading a stale cache. The fix guarantees that as long as the caller continues to invoke
        // the refresh with the current instant, the recency gate flips from unsafe to safe on its
        // own once the settle buffer elapses, without any manual cache-file deletion.
        assertFalse(isCacheSafeForDay(BLOCK_DAY, justAfterMidnight));
        assertTrue(isCacheSafeForDay(BLOCK_DAY, justAfterBuffer));
    }
}
