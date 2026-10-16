// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import org.hiero.block.api.RangedAddressBookHistory;
import org.hiero.block.api.RangedNodeAddressBook;
import org.hiero.block.api.TssData;
import org.hiero.block.node.app.fixtures.plugintest.TestApplicationStateFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/// Tests for {@link VerificationDataProvider}.
@DisplayName("VerificationDataProvider Tests")
class VerificationDataProviderTest {

    private TestApplicationStateFacility stateFacility;
    private VerificationDataProvider provider;

    @BeforeEach
    void setUp() {
        stateFacility = new TestApplicationStateFacility();
        final BlockNodeContext context =
                new BlockNodeContext(null, null, null, null, null, stateFacility, null, null, null);
        provider = new VerificationDataProvider(context);
    }

    // ─── rsaPublicKeysForBlock ────────────────────────────────────────────────

    @Test
    @DisplayName("rsaPublicKeysForBlock: returns empty Map when no address book history set")
    void rsaPublicKeysForBlock_nullWhenNoHistory() {
        // stateFacility has no history set → getAddressBookForBlock returns empty Map
        assertThat(provider.rsaPublicKeysForBlock(0L)).isEmpty();
    }

    @Test
    @DisplayName("rsaPublicKeysForBlock: returns an empty Map when block not covered by any era")
    void rsaPublicKeysForBlock_nullWhenBlockNotInAnyEra() {
        // era covers block 100-200; querying block 0 → empty Map
        final NodeAddressBook book = NodeAddressBook.newBuilder().build();
        final RangedAddressBookHistory history = RangedAddressBookHistory.newBuilder()
                .addressBooks(RangedNodeAddressBook.newBuilder()
                        .startBlock(100L)
                        .endBlock(200L)
                        .addressBook(book)
                        .build())
                .build();
        stateFacility.setAddressBookHistory(history);
        assertThat(provider.rsaPublicKeysForBlock(0L)).isEmpty();
    }

    @Test
    @DisplayName("rsaPublicKeysForBlock: returns empty map for era with no node addresses")
    void rsaPublicKeysForBlock_emptyMapWhenBookHasNoAddresses() {
        // era covers all blocks (open-ended), address book has no entries → empty map
        final NodeAddressBook emptyBook = NodeAddressBook.newBuilder().build();
        final RangedAddressBookHistory history = RangedAddressBookHistory.newBuilder()
                .addressBooks(RangedNodeAddressBook.newBuilder()
                        .startBlock(0L)
                        .endBlock(-1L)
                        .addressBook(emptyBook)
                        .build())
                .build();
        stateFacility.setAddressBookHistory(history);
        assertThat(provider.rsaPublicKeysForBlock(0L)).isEmpty();
    }

    @Test
    @DisplayName("rsaPublicKeysForBlock: skips addresses with blank RSA key")
    void rsaPublicKeysForBlock_skipsBlankRsaKey() {
        // node address with blank rsaPubKey → skipped → empty map returned
        final NodeAddress addr =
                NodeAddress.newBuilder().nodeId(1L).rsaPubKey("").build();
        final NodeAddressBook book =
                NodeAddressBook.newBuilder().nodeAddress(addr).build();
        final RangedAddressBookHistory history = RangedAddressBookHistory.newBuilder()
                .addressBooks(RangedNodeAddressBook.newBuilder()
                        .startBlock(0L)
                        .endBlock(-1L)
                        .addressBook(book)
                        .build())
                .build();
        stateFacility.setAddressBookHistory(history);
        assertThat(provider.rsaPublicKeysForBlock(42L)).isEmpty();
    }

    @Test
    @DisplayName("rsaPublicKeysForBlock: skips addresses with malformed RSA key")
    void rsaPublicKeysForBlock_skipsMalformedRsaKey() {
        // node address with invalid hex key → skipped → empty map
        final NodeAddress addr =
                NodeAddress.newBuilder().nodeId(2L).rsaPubKey("notvalidhex!!").build();
        final NodeAddressBook book =
                NodeAddressBook.newBuilder().nodeAddress(addr).build();
        final RangedAddressBookHistory history = RangedAddressBookHistory.newBuilder()
                .addressBooks(RangedNodeAddressBook.newBuilder()
                        .startBlock(0L)
                        .endBlock(-1L)
                        .addressBook(book)
                        .build())
                .build();
        stateFacility.setAddressBookHistory(history);
        assertThat(provider.rsaPublicKeysForBlock(0L)).isEmpty();
    }

    @Test
    @DisplayName("rsaPublicKeysForBlock: same map instance returned for repeated calls in same era")
    void rsaPublicKeysForBlock_sameInstanceOnCacheHit() {
        // open-ended era covering all blocks
        final NodeAddressBook emptyBook = NodeAddressBook.newBuilder().build();
        final RangedAddressBookHistory history = RangedAddressBookHistory.newBuilder()
                .addressBooks(RangedNodeAddressBook.newBuilder()
                        .startBlock(0L)
                        .endBlock(-1L)
                        .addressBook(emptyBook)
                        .build())
                .build();
        stateFacility.setAddressBookHistory(history);
        final var first = provider.rsaPublicKeysForBlock(0L);
        final var second = provider.rsaPublicKeysForBlock(1L);
        assertThat(second).isSameAs(first);
    }

    // ─── safeUpdateTssData ────────────────────────────────────────────────────

    @Test
    @DisplayName("safeUpdateTssData: null input is a no-op")
    void safeUpdateTssData_nullIsNoOp() {
        provider.safeUpdateTssData(null, false);
        assertThat(provider.hasTssData()).isFalse();
        assertThat(provider.currentTssData()).isNull();
    }

    @Test
    @DisplayName("hasTssData: returns false when no TSS data set")
    void hasTssData_falseWhenNoData() {
        assertThat(provider.hasTssData()).isFalse();
    }

    @Test
    @DisplayName("currentTssData: returns null when no TSS data set")
    void currentTssData_nullWhenNoData() {
        assertThat(provider.currentTssData()).isNull();
    }
    /// Tests of [VerificationDataProvider#safeUpdateTssData] installing TSS data. Nothing is installed into
    /// the TSS library any more, so these tests need no native code and use arbitrary ledger id bytes.
    @Nested
    @DisplayName("TSS data updates")
    class TssDataUpdateTests {
        /// This test aims to assert that TSS data carrying only a ledger id and the block it is valid from,
        /// without the informational roster, is installed: the roster is no longer needed to verify.
        @Test
        @DisplayName("TSS data without a roster is installed")
        void installsTssDataWithoutRoster() {
            final TssData tssData = tssData(1, 0L);

            provider.safeUpdateTssData(tssData, false);

            assertThat(provider.hasTssData()).isTrue();
            assertThat(provider.currentTssData()).isEqualTo(tssData);
        }

        /// This test aims to assert that an installed update is forwarded to the application state facility
        /// only when requested, which is how block 0 self-provisioning persists the data while updates that
        /// came from the facility are not sent back to it.
        @Test
        @DisplayName("an installed update is forwarded to the application state only when requested")
        void forwardsToApplicationStateOnlyWhenRequested() {
            provider.safeUpdateTssData(tssData(1, 0L), false);
            assertThat(stateFacility.getTssDataUpdates()).isEmpty();

            final TssData newer = tssData(2, 10L);
            provider.safeUpdateTssData(newer, true);

            assertThat(stateFacility.getTssDataUpdates()).containsExactly(newer);
        }

        /// This test aims to assert that TSS data valid from a later block replaces the current data.
        @Test
        @DisplayName("TSS data valid from a later block replaces the current data")
        void laterValidFromBlockReplaces() {
            provider.safeUpdateTssData(tssData(1, 0L), false);
            final TssData newer = tssData(2, 10L);

            provider.safeUpdateTssData(newer, false);

            assertThat(provider.currentTssData()).isEqualTo(newer);
        }

        /// This test aims to assert that different TSS data valid from the same block as the current data is
        /// ignored: only a strictly later `valid_from_block` replaces installed data, so data from a peer
        /// cannot overwrite what the node already holds for that block.
        @Test
        @DisplayName("different TSS data valid from the same block is ignored")
        void sameValidFromBlockIsIgnored() {
            final TssData current = tssData(1, 10L);
            provider.safeUpdateTssData(current, false);

            provider.safeUpdateTssData(tssData(2, 10L), false);

            assertThat(provider.currentTssData()).isEqualTo(current);
        }

        /// This test aims to assert that TSS data valid from an earlier block than the current data is
        /// ignored.
        @Test
        @DisplayName("TSS data valid from an earlier block is ignored")
        void earlierValidFromBlockIsIgnored() {
            final TssData current = tssData(1, 10L);
            provider.safeUpdateTssData(current, false);

            provider.safeUpdateTssData(tssData(2, 5L), false);

            assertThat(provider.currentTssData()).isEqualTo(current);
        }

        private static TssData tssData(final int ledgerIdSeed, final long validFromBlock) {
            return TssData.newBuilder()
                    .ledgerId(Bytes.wrap(new byte[] {(byte) ledgerIdSeed, 7, 7, 7}))
                    .validFromBlock(validFromBlock)
                    .build();
        }
    }
}
