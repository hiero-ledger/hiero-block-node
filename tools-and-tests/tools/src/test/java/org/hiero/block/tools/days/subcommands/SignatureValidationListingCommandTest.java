// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.subcommands;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for GcpLsCommand.
 *
 * These tests focus on the core comparison logic (compareHour) by
 * injecting synthetic GCP/local summaries via a small test subclass and
 * calling the real private compareHour(...) via reflection.
 *
 * We deliberately avoid exercising real GCP or tar.zst IO here; that can
 * be covered by higher-level integration tests.
 */
class SignatureValidationListingCommandTest {

    /**
     * Simple test subclass that lets us inject SigSummary data by overriding the summary builder methods.
     */
    private static final class TestableSignatureValidationListingCommand extends SignatureValidationListingCommand {

        // Test data to be used by compareHour
        Map<String, Integer> gcpCounts;
        Map<String, List<String>> gcpFiles;
        Map<String, Integer> localCounts;
        Map<String, List<String>> localFiles;

        @Override
        public SigSummary buildGcpSigSummary(LocalDate date, int hour) {
            return new SigSummary(gcpCounts, gcpFiles);
        }

        @Override
        public SigSummary buildLocalSigSummary(LocalDate date, int hour) {
            return new SigSummary(localCounts, localFiles);
        }
    }

    @Test
    void compareHour_buildsDiffsAndFileListsCorrectly() throws Exception {
        LocalDate date = LocalDate.of(2019, 9, 13);
        int hour = 21;

        // For ts1, GCP and local match (2 sigs each)
        // For ts2, GCP has 3, local has 1 (mismatch)
        // For ts3, only local has sigs (mismatch)
        Map<String, Integer> gcpCounts = Map.of(
                "2019-09-13T21_53_51.000000Z", 2,
                "2019-09-13T21_54_00.000000Z", 3);
        Map<String, List<String>> gcpFiles = Map.of(
                "2019-09-13T21_53_51.000000Z",
                List.of(
                        "recordstreams/record0.0.3/2019-09-13T21_53_51.000000Z.rcd_sig",
                        "recordstreams/record0.0.4/2019-09-13T21_53_51.000000Z.rcd_sig"),
                "2019-09-13T21_54_00.000000Z",
                List.of(
                        "recordstreams/record0.0.3/2019-09-13T21_54_00.000000Z.rcd_sig",
                        "recordstreams/record0.0.4/2019-09-13T21_54_00.000000Z.rcd_sig",
                        "recordstreams/record0.0.5/2019-09-13T21_54_00.000000Z.rcd_sig"));

        Map<String, Integer> localCounts = Map.of(
                "2019-09-13T21_53_51.000000Z", 2,
                "2019-09-13T21_54_00.000000Z", 1,
                "2019-09-13T21_55_00.000000Z", 4);
        Map<String, List<String>> localFiles = Map.of(
                "2019-09-13T21_53_51.000000Z",
                List.of(
                        "local-node3-2019-09-13T21_53_51.000000Z.rcd_sig",
                        "local-node4-2019-09-13T21_53_51.000000Z.rcd_sig"),
                "2019-09-13T21_54_00.000000Z",
                List.of("local-node3-2019-09-13T21_54_00.000000Z.rcd_sig"),
                "2019-09-13T21_55_00.000000Z",
                List.of(
                        "local-node3-2019-09-13T21_55_00.000000Z.rcd_sig",
                        "local-node4-2019-09-13T21_55_00.000000Z.rcd_sig",
                        "local-node5-2019-09-13T21_55_00.000000Z.rcd_sig",
                        "local-node6-2019-09-13T21_55_00.000000Z.rcd_sig"));

        // Wire our test command with synthetic summaries
        TestableSignatureValidationListingCommand cmd = new TestableSignatureValidationListingCommand();
        cmd.gcpCounts = gcpCounts;
        cmd.gcpFiles = gcpFiles;
        cmd.localCounts = localCounts;
        cmd.localFiles = localFiles;

        SignatureValidationListingCommand.HourResult result = cmd.compareHour(date, hour);

        // We expect one matching timestamp (ts1) and two mismatches (ts2, ts3)
        assertEquals(1, result.sameCount);
        assertEquals(2, result.diffs.size());

        // ts2 mismatch (GCP 3, local 1)
        SignatureValidationListingCommand.PerTimestampDiff ts2 = result.diffs.stream()
                .filter(d -> d.timestamp.equals("2019-09-13T21_54_00.000000Z"))
                .findFirst()
                .orElseThrow();
        assertEquals(3, ts2.gcpCount);
        assertEquals(1, ts2.localCount);
        assertEquals(gcpFiles.get("2019-09-13T21_54_00.000000Z"), ts2.gcpFiles);
        assertEquals(localFiles.get("2019-09-13T21_54_00.000000Z"), ts2.localFiles);

        // ts3 mismatch (GCP 0, local 4)
        SignatureValidationListingCommand.PerTimestampDiff ts3 = result.diffs.stream()
                .filter(d -> d.timestamp.equals("2019-09-13T21_55_00.000000Z"))
                .findFirst()
                .orElseThrow();
        assertEquals(0, ts3.gcpCount);
        assertEquals(4, ts3.localCount);
        assertTrue(ts3.gcpFiles.isEmpty());
        assertEquals(localFiles.get("2019-09-13T21_55_00.000000Z"), ts3.localFiles);
    }
}
