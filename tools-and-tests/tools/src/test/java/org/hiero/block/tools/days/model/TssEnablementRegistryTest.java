// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.node.tss.LedgerIdNodeContribution;
import com.hedera.hapi.node.tss.LedgerIdPublicationTransactionBody;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.hiero.block.api.TssData;
import org.hiero.block.internal.DatedTssPublication;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("TssEnablementRegistry")
class TssEnablementRegistryTest {

    private static final Bytes LEDGER_ID = Bytes.wrap(new byte[] {1, 2, 3, 4});
    private static final Bytes WRAPS_VK = Bytes.wrap(new byte[] {10, 20, 30, 40, 50});
    private static final Bytes HISTORY_PROOF_KEY_0 = Bytes.wrap(new byte[] {0xA, 0xB});
    private static final Bytes HISTORY_PROOF_KEY_1 = Bytes.wrap(new byte[] {0xC, 0xD});

    private static LedgerIdPublicationTransactionBody createPublication(
            Bytes ledgerId, Bytes wrapsVk, long... nodeIdWeightPairs) {
        List<LedgerIdNodeContribution> contributions = new ArrayList<>();
        for (int i = 0; i < nodeIdWeightPairs.length; i += 2) {
            contributions.add(LedgerIdNodeContribution.newBuilder()
                    .nodeId(nodeIdWeightPairs[i])
                    .weight(nodeIdWeightPairs[i + 1])
                    .historyProofKey(i == 0 ? HISTORY_PROOF_KEY_0 : HISTORY_PROOF_KEY_1)
                    .build());
        }
        return LedgerIdPublicationTransactionBody.newBuilder()
                .ledgerId(ledgerId)
                .historyProofVerificationKey(wrapsVk)
                .nodeContributions(contributions)
                .build();
    }

    @Nested
    @DisplayName("empty registry")
    class EmptyRegistry {

        @Test
        @DisplayName("reports no TSS data")
        void hasNoTssData() {
            TssEnablementRegistry registry = new TssEnablementRegistry();
            assertFalse(registry.hasTssData());
            assertEquals(0, registry.getPublicationCount());
        }

        @Test
        @DisplayName("returns null latest publication")
        void returnsNullLatest() {
            TssEnablementRegistry registry = new TssEnablementRegistry();
            assertNull(registry.getLatestPublication());
            assertNull(registry.getLatestTssData());
        }
    }

    @Nested
    @DisplayName("recordPublication")
    class RecordPublication {

        @Test
        @DisplayName("records a publication and returns description")
        void recordsPublication() {
            TssEnablementRegistry registry = new TssEnablementRegistry();
            LedgerIdPublicationTransactionBody body = createPublication(LEDGER_ID, WRAPS_VK, 0, 1000, 1, 2000);
            String result = registry.recordPublication(Instant.ofEpochSecond(100), 42, body);
            assertNotNull(result);
            assertTrue(result.contains("WRAPS VK size=5 bytes"));
            assertTrue(result.contains("roster=2 nodes"));
            assertTrue(result.contains("node 0: weight=1000"));
            assertTrue(result.contains("node 1: weight=2000"));
            assertTrue(registry.hasTssData());
            assertEquals(1, registry.getPublicationCount());
        }

        @Test
        @DisplayName("appends multiple publications")
        void appendsMultiple() {
            TssEnablementRegistry registry = new TssEnablementRegistry();
            registry.recordPublication(Instant.ofEpochSecond(100), 10, createPublication(LEDGER_ID, WRAPS_VK, 0, 1000));
            registry.recordPublication(Instant.ofEpochSecond(200), 20, createPublication(LEDGER_ID, WRAPS_VK, 0, 2000));
            assertEquals(2, registry.getPublicationCount());
        }
    }

    @Nested
    @DisplayName("getLatestTssData")
    class GetLatest {

        @Test
        @DisplayName("returns the most recent publication")
        void returnsMostRecent() {
            TssEnablementRegistry registry = new TssEnablementRegistry();
            registry.recordPublication(Instant.ofEpochSecond(100), 10, createPublication(LEDGER_ID, WRAPS_VK, 0, 1000));
            registry.recordPublication(Instant.ofEpochSecond(200), 20, createPublication(LEDGER_ID, WRAPS_VK, 0, 2000));
            DatedTssPublication latest = registry.getLatestPublication();
            assertNotNull(latest);
            assertEquals(20, latest.blockNumber());
        }

        @Test
        @DisplayName("getLatestTssData converts correctly to TssData format")
        void convertsTssDataCorrectly() {
            TssEnablementRegistry registry = new TssEnablementRegistry();
            LedgerIdPublicationTransactionBody original = createPublication(LEDGER_ID, WRAPS_VK, 0, 1000, 1, 2000);
            registry.recordPublication(Instant.ofEpochSecond(100), 42, original);

            TssData tssData = registry.getLatestTssData();
            assertNotNull(tssData);
            assertEquals(LEDGER_ID, tssData.ledgerId());
            assertEquals(WRAPS_VK, tssData.wrapsVerificationKey());
            assertEquals(2, tssData.currentRosterOrThrow().rosterEntries().size());
            assertEquals(
                    0, tssData.currentRosterOrThrow().rosterEntries().getFirst().nodeId());
            assertEquals(
                    1000,
                    tssData.currentRosterOrThrow().rosterEntries().getFirst().weight());
            assertEquals(
                    HISTORY_PROOF_KEY_0,
                    tssData.currentRosterOrThrow().rosterEntries().getFirst().schnorrPublicKey());
            assertEquals(42, tssData.validFromBlock());
            assertEquals(42, tssData.currentRosterOrThrow().validFromBlock());
        }
    }

    @Nested
    @DisplayName("persistence")
    class Persistence {

        @Test
        @DisplayName("save and reload preserves data")
        void saveAndReloadPreservesData(@TempDir Path tempDir) {
            TssEnablementRegistry registry = new TssEnablementRegistry();
            registry.recordPublication(
                    Instant.ofEpochSecond(100), 10, createPublication(LEDGER_ID, WRAPS_VK, 0, 1000, 1, 2000));
            registry.recordPublication(
                    Instant.ofEpochSecond(200), 20, createPublication(LEDGER_ID, WRAPS_VK, 0, 3000, 1, 4000));

            Path file = tempDir.resolve("tssPublicationHistory.json");
            registry.saveToJsonFile(file);

            TssEnablementRegistry loaded = new TssEnablementRegistry();
            loaded.reloadFromFile(file);
            assertEquals(2, loaded.getPublicationCount());
            DatedTssPublication latest = loaded.getLatestPublication();
            assertNotNull(latest);
            assertEquals(20, latest.blockNumber());
            assertEquals(LEDGER_ID, latest.tssData().ledgerId());
        }

        @Test
        @DisplayName("reloadFromFile replaces existing data")
        void reloadReplacesData(@TempDir Path tempDir) {
            TssEnablementRegistry original = new TssEnablementRegistry();
            original.recordPublication(Instant.ofEpochSecond(100), 10, createPublication(LEDGER_ID, WRAPS_VK, 0, 1000));
            Path file = tempDir.resolve("tssPublicationHistory.json");
            original.saveToJsonFile(file);

            TssEnablementRegistry registry = new TssEnablementRegistry();
            registry.recordPublication(Instant.ofEpochSecond(50), 5, createPublication(LEDGER_ID, WRAPS_VK, 0, 500));
            assertEquals(1, registry.getPublicationCount());

            registry.reloadFromFile(file);
            assertEquals(1, registry.getPublicationCount());
            assertEquals(10, registry.getLatestPublication().blockNumber());
        }
    }

    @Nested
    @DisplayName("writeTssParametersBin")
    class WriteBin {

        @Test
        @DisplayName("writes parseable TssData protobuf binary")
        void writesParseableProtobuf(@TempDir Path tempDir) throws Exception {
            TssEnablementRegistry registry = new TssEnablementRegistry();
            LedgerIdPublicationTransactionBody original = createPublication(LEDGER_ID, WRAPS_VK, 0, 1000, 1, 2000);
            registry.recordPublication(Instant.ofEpochSecond(100), 42, original);

            Path binFile = tempDir.resolve("tss-enablement.bin");
            registry.writeTssParametersBin(binFile);

            assertTrue(Files.exists(binFile));
            Bytes fileBytes = Bytes.wrap(Files.readAllBytes(binFile));
            TssData parsed = TssData.PROTOBUF.parse(fileBytes);
            assertEquals(LEDGER_ID, parsed.ledgerId());
            assertEquals(WRAPS_VK, parsed.wrapsVerificationKey());
            assertEquals(2, parsed.currentRosterOrThrow().rosterEntries().size());
        }

        @Test
        @DisplayName("does nothing when registry is empty")
        void doesNothingWhenEmpty(@TempDir Path tempDir) throws Exception {
            TssEnablementRegistry registry = new TssEnablementRegistry();
            Path binFile = tempDir.resolve("tss-enablement.bin");
            registry.writeTssParametersBin(binFile);
            assertFalse(Files.exists(binFile));
        }
    }
}
