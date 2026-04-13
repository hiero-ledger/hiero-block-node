// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests for {@link SignatureStatsCollector}. */
class SignatureStatsCollectorTest {

    @Nested
    @DisplayName("Per-day aggregation")
    class PerDayAggregation {

        @Test
        @DisplayName("single block creates one day entry")
        void singleBlockCreatesOneDayEntry(@TempDir Path tempDir) {
            Path csvFile = tempDir.resolve("stats.csv");
            SignatureStatsCollector collector = new SignatureStatsCollector(csvFile);

            Instant blockTime = Instant.parse("2023-06-15T12:00:00Z");
            collector.accept(new SignatureBlockStats(
                    100,
                    blockTime,
                    true,
                    5,
                    1500,
                    500,
                    Set.of(0L, 1L, 2L),
                    600,
                    5,
                    Map.of(0L, 100L, 1L, 200L, 2L, 300L, 3L, 400L, 4L, 500L),
                    Map.of(
                            0L, SignatureBlockStats.NodeResult.VERIFIED,
                            1L, SignatureBlockStats.NodeResult.VERIFIED,
                            2L, SignatureBlockStats.NodeResult.VERIFIED,
                            3L, SignatureBlockStats.NodeResult.NOT_PRESENT,
                            4L, SignatureBlockStats.NodeResult.NOT_PRESENT)));

            collector.finalizeDayStats();
            collector.close();

            assertTrue(Files.exists(csvFile));
        }

        @Test
        @DisplayName("multiple blocks same day aggregated together")
        void multipleBlocksSameDayAggregated(@TempDir Path tempDir) throws IOException {
            Path csvFile = tempDir.resolve("stats.csv");
            SignatureStatsCollector collector = new SignatureStatsCollector(csvFile);

            Instant time1 = Instant.parse("2023-06-15T10:00:00Z");
            Instant time2 = Instant.parse("2023-06-15T14:00:00Z");
            Map<Long, Long> stakeMap = Map.of(0L, 100L, 1L, 200L, 2L, 300L);

            collector.accept(new SignatureBlockStats(
                    1,
                    time1,
                    true,
                    3,
                    600,
                    200,
                    Set.of(0L, 1L),
                    300,
                    3,
                    stakeMap,
                    Map.of(
                            0L, SignatureBlockStats.NodeResult.VERIFIED,
                            1L, SignatureBlockStats.NodeResult.VERIFIED,
                            2L, SignatureBlockStats.NodeResult.NOT_PRESENT)));

            collector.accept(new SignatureBlockStats(
                    2,
                    time2,
                    true,
                    3,
                    600,
                    200,
                    Set.of(0L, 1L, 2L),
                    600,
                    3,
                    stakeMap,
                    Map.of(
                            0L, SignatureBlockStats.NodeResult.VERIFIED,
                            1L, SignatureBlockStats.NodeResult.VERIFIED,
                            2L, SignatureBlockStats.NodeResult.VERIFIED)));

            collector.finalizeDayStats();
            collector.close();

            String csv = Files.readString(csvFile);
            String[] lines = csv.split("\n");
            assertEquals(2, lines.length, "Should have header + 1 day row");
            assertTrue(lines[1].startsWith("2023-06-15"), "Row should start with the day date");
        }

        @Test
        @DisplayName("blocks on different days create separate entries")
        void differentDaysCreateSeparateEntries(@TempDir Path tempDir) throws IOException {
            Path csvFile = tempDir.resolve("stats.csv");
            SignatureStatsCollector collector = new SignatureStatsCollector(csvFile);

            Map<Long, Long> stakeMap = Map.of(0L, 100L, 1L, 200L);

            collector.accept(new SignatureBlockStats(
                    1,
                    Instant.parse("2023-06-15T12:00:00Z"),
                    true,
                    2,
                    300,
                    100,
                    Set.of(0L, 1L),
                    300,
                    2,
                    stakeMap,
                    Map.of(
                            0L, SignatureBlockStats.NodeResult.VERIFIED,
                            1L, SignatureBlockStats.NodeResult.VERIFIED)));

            collector.accept(new SignatureBlockStats(
                    2,
                    Instant.parse("2023-06-16T12:00:00Z"),
                    true,
                    2,
                    300,
                    100,
                    Set.of(0L, 1L),
                    300,
                    2,
                    stakeMap,
                    Map.of(
                            0L, SignatureBlockStats.NodeResult.VERIFIED,
                            1L, SignatureBlockStats.NodeResult.VERIFIED)));

            collector.finalizeDayStats();
            collector.close();

            String csv = Files.readString(csvFile);
            String[] lines = csv.split("\n");
            assertEquals(3, lines.length, "Should have header + 2 day rows");
            assertTrue(lines[1].startsWith("2023-06-15"));
            assertTrue(lines[2].startsWith("2023-06-16"));
        }
    }

    @Nested
    @DisplayName("CSV output")
    class CsvOutput {

        @Test
        @DisplayName("header includes stake weight columns")
        void headerIncludesStakeWeightColumns(@TempDir Path tempDir) throws IOException {
            Path csvFile = tempDir.resolve("stats.csv");
            SignatureStatsCollector collector = new SignatureStatsCollector(csvFile);

            collector.accept(new SignatureBlockStats(
                    1,
                    Instant.parse("2023-06-15T12:00:00Z"),
                    true,
                    3,
                    600,
                    200,
                    Set.of(0L, 1L),
                    300,
                    3,
                    Map.of(0L, 100L, 1L, 200L, 2L, 300L),
                    Map.of(
                            0L, SignatureBlockStats.NodeResult.VERIFIED,
                            1L, SignatureBlockStats.NodeResult.VERIFIED,
                            2L, SignatureBlockStats.NodeResult.NOT_PRESENT)));

            collector.finalizeDayStats();
            collector.close();

            String header = Files.readAllLines(csvFile).getFirst();
            assertTrue(header.contains("mode"), "Header should contain mode column");
            assertTrue(header.contains("total_stake"), "Header should contain total_stake column");
            assertTrue(header.contains("min_validated_stake_pct"));
            assertTrue(header.contains("max_validated_stake_pct"));
            assertTrue(header.contains("mean_validated_stake_pct"));
            assertTrue(header.contains("closest_threshold_margin"));
            assertTrue(header.contains("min_signing_nodes"));
            assertTrue(header.contains("max_signing_nodes"));
            assertTrue(header.contains("mean_signing_nodes"));
            assertTrue(header.contains("min_node_stake"));
            assertTrue(header.contains("max_node_stake"));
            assertTrue(header.contains("mean_node_stake"));
            assertTrue(header.contains("median_node_stake"));
            assertTrue(header.contains("std_dev_node_stake"));
            assertTrue(header.contains("top3_concentration_pct"));
            assertTrue(header.contains("missing_signers"));
            assertTrue(header.contains("reliable_signers"));
        }

        @Test
        @DisplayName("stake-weighted row contains mode and stake data")
        void stakeWeightedRowContainsModeAndStakeData(@TempDir Path tempDir) throws IOException {
            Path csvFile = tempDir.resolve("stats.csv");
            SignatureStatsCollector collector = new SignatureStatsCollector(csvFile);

            collector.accept(new SignatureBlockStats(
                    1,
                    Instant.parse("2023-06-15T12:00:00Z"),
                    true,
                    3,
                    600,
                    200,
                    Set.of(0L, 1L, 2L),
                    600,
                    3,
                    Map.of(0L, 100L, 1L, 200L, 2L, 300L),
                    Map.of(
                            0L, SignatureBlockStats.NodeResult.VERIFIED,
                            1L, SignatureBlockStats.NodeResult.VERIFIED,
                            2L, SignatureBlockStats.NodeResult.VERIFIED)));

            collector.finalizeDayStats();
            collector.close();

            String dataRow = Files.readAllLines(csvFile).get(1);
            assertTrue(dataRow.contains("stake-weighted"), "Row should contain stake-weighted mode");
            assertTrue(dataRow.contains("600"), "Row should contain total stake");
        }

        @Test
        @DisplayName("null csv path does not throw")
        void nullCsvPathDoesNotThrow() {
            assertDoesNotThrow(() -> {
                SignatureStatsCollector collector = new SignatureStatsCollector(null);
                collector.accept(new SignatureBlockStats(
                        1,
                        Instant.parse("2023-06-15T12:00:00Z"),
                        false,
                        3,
                        3,
                        2,
                        Set.of(0L, 1L),
                        2,
                        3,
                        Map.of(),
                        Map.of(
                                0L, SignatureBlockStats.NodeResult.VERIFIED,
                                1L, SignatureBlockStats.NodeResult.VERIFIED,
                                2L, SignatureBlockStats.NodeResult.NOT_PRESENT)));
                collector.finalizeDayStats();
                collector.printFinalSummary();
                collector.close();
            });
        }
    }

    @Nested
    @DisplayName("Stake weight distribution")
    class StakeWeightDistribution {

        @Test
        @DisplayName("min/max/mean/median calculated correctly")
        void stakeDistributionCalculatedCorrectly(@TempDir Path tempDir) throws IOException {
            Path csvFile = tempDir.resolve("stats.csv");
            SignatureStatsCollector collector = new SignatureStatsCollector(csvFile);

            // Stakes: 100, 200, 300 -> min=100, max=300, mean=200, median=200
            collector.accept(new SignatureBlockStats(
                    1,
                    Instant.parse("2023-06-15T12:00:00Z"),
                    true,
                    3,
                    600,
                    200,
                    Set.of(0L, 1L, 2L),
                    600,
                    3,
                    Map.of(0L, 100L, 1L, 200L, 2L, 300L),
                    Map.of(
                            0L, SignatureBlockStats.NodeResult.VERIFIED,
                            1L, SignatureBlockStats.NodeResult.VERIFIED,
                            2L, SignatureBlockStats.NodeResult.VERIFIED)));

            collector.finalizeDayStats();
            collector.close();

            String dataRow = Files.readAllLines(csvFile).get(1);
            assertTrue(dataRow.contains(",100,"), "Should contain min stake of 100");
            assertTrue(dataRow.contains(",300,"), "Should contain max stake of 300");
        }

        @Test
        @DisplayName("top3 concentration calculates correctly with fewer than 3 nodes")
        void top3ConcentrationWithFewerThan3Nodes(@TempDir Path tempDir) throws IOException {
            Path csvFile = tempDir.resolve("stats.csv");
            SignatureStatsCollector collector = new SignatureStatsCollector(csvFile);

            // Only 2 nodes -> top3 = both = 100%
            collector.accept(new SignatureBlockStats(
                    1,
                    Instant.parse("2023-06-15T12:00:00Z"),
                    true,
                    2,
                    500,
                    167,
                    Set.of(0L, 1L),
                    500,
                    2,
                    Map.of(0L, 200L, 1L, 300L),
                    Map.of(
                            0L, SignatureBlockStats.NodeResult.VERIFIED,
                            1L, SignatureBlockStats.NodeResult.VERIFIED)));

            collector.finalizeDayStats();
            collector.close();

            String dataRow = Files.readAllLines(csvFile).get(1);
            assertTrue(dataRow.contains("100.0000"), "Top3 with 2 nodes should be 100%");
        }
    }

    @Nested
    @DisplayName("Missing and reliable signers")
    class MissingAndReliableSigners {

        @Test
        @DisplayName("node that never signs is counted as missing")
        void nodeThatNeverSignsIsMissing(@TempDir Path tempDir) throws IOException {
            Path csvFile = tempDir.resolve("stats.csv");
            SignatureStatsCollector collector = new SignatureStatsCollector(csvFile);

            Map<Long, Long> stakeMap = Map.of(0L, 100L, 1L, 200L, 2L, 300L);
            Map<Long, SignatureBlockStats.NodeResult> results = Map.of(
                    0L, SignatureBlockStats.NodeResult.VERIFIED,
                    1L, SignatureBlockStats.NodeResult.VERIFIED,
                    2L, SignatureBlockStats.NodeResult.NOT_PRESENT);

            // Node 2 never signs across 2 blocks
            collector.accept(new SignatureBlockStats(
                    1,
                    Instant.parse("2023-06-15T10:00:00Z"),
                    true,
                    3,
                    600,
                    200,
                    Set.of(0L, 1L),
                    300,
                    2,
                    stakeMap,
                    results));

            collector.accept(new SignatureBlockStats(
                    2,
                    Instant.parse("2023-06-15T14:00:00Z"),
                    true,
                    3,
                    600,
                    200,
                    Set.of(0L, 1L),
                    300,
                    2,
                    stakeMap,
                    results));

            collector.finalizeDayStats();
            collector.close();

            String dataRow = Files.readAllLines(csvFile).get(1);
            assertTrue(dataRow.endsWith(",1,2"), "Should end with 1 missing, 2 reliable signers");
        }
    }

    @Nested
    @DisplayName("SignatureBlockStats.fromPreVerifiedData")
    class FromPreVerifiedData {

        @Test
        @DisplayName("builds stats correctly from pre-verified data")
        void buildsStatsFromPreVerifiedData() {
            SignatureBlockStats stats = SignatureBlockStats.fromPreVerifiedData(
                    42,
                    Instant.parse("2023-06-15T12:00:00Z"),
                    List.of(0L, 1L, 2L),
                    3,
                    List.of(0L, 1L, 2L, 3L, 4L),
                    Map.of(0L, 100L, 1L, 200L, 2L, 300L, 3L, 400L, 4L, 500L));

            assertEquals(42, stats.blockNumber());
            assertTrue(stats.stakeWeighted());
            assertEquals(5, stats.totalNodes());
            assertEquals(1500, stats.totalStake());
            assertEquals(500, stats.threshold());
            assertEquals(Set.of(0L, 1L, 2L), stats.validatedNodes());
            assertEquals(600, stats.validatedStake());
            assertEquals(3, stats.totalSignatures());
            assertEquals(5, stats.perNodeResults().size());
            assertEquals(
                    SignatureBlockStats.NodeResult.VERIFIED,
                    stats.perNodeResults().get(0L));
            assertEquals(
                    SignatureBlockStats.NodeResult.NOT_PRESENT,
                    stats.perNodeResults().get(3L));
        }

        @Test
        @DisplayName("null stake map falls back to equal-weight")
        void nullStakeMapFallsBackToEqualWeight() {
            SignatureBlockStats stats = SignatureBlockStats.fromPreVerifiedData(
                    1, Instant.parse("2023-06-15T12:00:00Z"), List.of(0L, 1L), 2, List.of(0L, 1L, 2L), null);

            assertFalse(stats.stakeWeighted());
            assertEquals(3, stats.totalNodes());
            assertEquals(3, stats.totalStake());
            assertEquals(2, stats.threshold());
            assertEquals(2, stats.validatedStake());
        }

        @Test
        @DisplayName("duplicate verified node IDs counted once")
        void duplicateVerifiedNodeIdsCountedOnce() {
            SignatureBlockStats stats = SignatureBlockStats.fromPreVerifiedData(
                    1,
                    Instant.parse("2023-06-15T12:00:00Z"),
                    List.of(0L, 0L, 1L),
                    3,
                    List.of(0L, 1L, 2L),
                    Map.of(0L, 100L, 1L, 200L, 2L, 300L));

            assertEquals(Set.of(0L, 1L), stats.validatedNodes());
            assertEquals(300, stats.validatedStake());
        }
    }
}
