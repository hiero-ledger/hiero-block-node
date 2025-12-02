// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.subcommands;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SignatureValidationListingCommand}.
 *
 * These tests focus on the core comparison logic (compareHour) by
 * injecting synthetic GCP/local summaries via a small test subclass.
 */
class SignatureValidationListingCommandTest {

    static class TestableSignatureValidationListingCommand extends SignatureValidationListingCommand {
        Map<Instant, Long> gcpCounts;
        Map<Instant, Long> localCounts;

        @Override
        protected Map<Instant, Long> buildGcpSigSummary(LocalDate date, int hour) {
            return gcpCounts;
        }

        @Override
        protected Map<Instant, Long> buildLocalSigSummary(LocalDate date, int hour) {
            return localCounts;
        }
    }

    @Test
    void compareHourBuildsDiffsAndCountsCorrectly() {
        LocalDate date = LocalDate.of(2019, 9, 13);
        int hour = 21;

        // ts1 matches, ts2 + ts3 are mismatches
        Instant ts1 = Instant.parse("2019-09-13T21:53:51Z");
        Instant ts2 = Instant.parse("2019-09-13T21:54:00Z");
        Instant ts3 = Instant.parse("2019-09-13T21:55:00Z");

        Map<Instant, Long> gcpCounts = Map.of(
                ts1, 2L,
                ts2, 3L);
        Map<Instant, Long> localCounts = Map.of(
                ts1, 2L,
                ts2, 1L,
                ts3, 4L);

        TestableSignatureValidationListingCommand cmd = new TestableSignatureValidationListingCommand();
        cmd.gcpCounts = gcpCounts;
        cmd.localCounts = localCounts;

        SignatureValidationListingCommand.HourResult result = cmd.compareHour(date, hour);

        // one match (ts1), two mismatches (ts2, ts3)
        assertEquals(1, result.sameCount());
        assertEquals(2, result.diffs().size());

        var ts2Diff = result.diffs().stream()
                .filter(d -> d.timestamp().equals(ts2))
                .findFirst()
                .orElseThrow();
        assertEquals(3L, ts2Diff.gcpCount());
        assertEquals(1L, ts2Diff.localCount());

        var ts3Diff = result.diffs().stream()
                .filter(d -> d.timestamp().equals(ts3))
                .findFirst()
                .orElseThrow();
        assertEquals(0L, ts3Diff.gcpCount());
        assertEquals(4L, ts3Diff.localCount());
    }
}
