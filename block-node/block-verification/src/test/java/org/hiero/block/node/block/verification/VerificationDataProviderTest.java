// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import org.hiero.block.api.RangedAddressBookHistory;
import org.hiero.block.api.RangedNodeAddressBook;
import org.hiero.block.node.app.fixtures.plugintest.TestApplicationStateFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Tests for {@link VerificationDataProvider}.
@DisplayName("VerificationDataProvider Tests")
class VerificationDataProviderTest {

    private TestApplicationStateFacility stateFacility;
    private VerificationDataProvider provider;

    @BeforeEach
    void setUp() {
        stateFacility = new TestApplicationStateFacility();
        final BlockNodeContext context = new BlockNodeContext(
                null, null, null, null, null, stateFacility, null, null, null, null, null, null, null);
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
}
