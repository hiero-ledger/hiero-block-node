// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

import com.hedera.cryptography.tss.TSS;
import com.hedera.cryptography.wraps.WRAPSVerificationKey;
import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Collections;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.hiero.block.api.RosterEntry;
import org.hiero.block.api.TssData;
import org.hiero.block.api.TssRoster;
import org.hiero.block.node.spi.BlockNodeContext;

/// Provider for verification data.
///
/// This provider provides [TssData] alongside with RSA public keys.
/// The provider also supports updates to the verification data.
public final class VerificationDataProvider {
    private static final System.Logger LOGGER = System.getLogger(VerificationDataProvider.class.getName());
    private final BlockNodeContext context;
    private final AtomicReference<TssData> currentTssData;
    /// One-entry cache: avoids re-parsing RSA keys for every block in the same address-book era.
    /// Held as an atomic pair so no thread can observe the new book alongside the old key map.
    private final AtomicReference<CachedKeyMap> cachedKeyMap;

    /// The cached pair of address book and its parsed key map.
    /// @param book the address book the keys were parsed from
    /// @param keys the parsed `node_id → PublicKey` map
    private record CachedKeyMap(NodeAddressBook book, Map<Long, PublicKey> keys) {}

    /// Constructor.
    ///
    /// @param context the block node context, for access to the application state facility,
    ///     must not be null
    public VerificationDataProvider(final BlockNodeContext context) {
        this.context = Objects.requireNonNull(context);
        this.currentTssData = new AtomicReference<>(null);
        this.cachedKeyMap = new AtomicReference<>(null);
    }

    /// Returns the currently available [TssData], or `null` if none has been received yet.
    ///
    /// @return the current [TssData], may be `null`
    public TssData currentTssData() {
        return currentTssData.get();
    }

    /// Returns `true` if TSS data is currently available.
    ///
    /// @return `true` when [#currentTssData()] would return a non-null value
    public boolean hasTssData() {
        return currentTssData.get() != null;
    }

    /// Returns the RSA public key map for the address book era that covers `blockNumber`,
    /// resolved via [org.hiero.block.node.spi.ApplicationStateFacility#getAddressBookForBlock].
    /// Returns an empty map when no era covers the block (the caller must fail the block with
    /// [org.hiero.block.node.block.verification.session.SessionFailureType#MISSING_VERIFICATION_DATA]).
    ///
    /// The result is cached by address-book identity: consecutive blocks in the same era share the
    /// same [NodeAddressBook] instance from the history lookup, so key parsing only happens once per
    /// era transition rather than once per block.
    ///
    /// @param blockNumber the number of the block to resolve keys for
    /// @return an unmodifiable `node_id → PublicKey` map, empty when no era covers the block
    public Map<Long, PublicKey> rsaPublicKeysForBlock(final long blockNumber) {

        final NodeAddressBook book = context.applicationStateFacility().getAddressBookForBlock(blockNumber);
        final CachedKeyMap cached = cachedKeyMap.get();
        if (cached != null && cached.book() == book) {
            return cached.keys();
        }
        try {
            final Map<Long, PublicKey> keys = buildKeyMap(book);
            if (!keys.isEmpty()) cachedKeyMap.set(new CachedKeyMap(book, keys));
            return keys;
        } catch (final NoSuchAlgorithmException e) {
            LOGGER.log(WARNING, "RSA KeyFactory not available for block {0} - returning empty key map", blockNumber);
            return Map.of();
        }
    }

    /// Safely update the TSS data. A `null` input and any runtime failure during
    /// the update are logged and swallowed; this method never throws.
    ///
    /// @param tssData the new TSS data, may be `null` in which case nothing happens
    /// @param sendUpdateToAppState if `true`, a successful update is also forwarded
    ///     to the application state facility
    public void safeUpdateTssData(final TssData tssData, final boolean sendUpdateToAppState) {
        try {
            if (tssData == null) {
                LOGGER.log(DEBUG, "No TSS data in current update");
            } else {
                updateTssData(tssData, sendUpdateToAppState);
            }
        } catch (final RuntimeException e) {
            LOGGER.log(WARNING, "Failed to update TSS data in verification", e);
        }
    }

    /// Update the TSS data if the given data is new and valid from a later block
    /// than the current data. On a successful update, the TSS library address book
    /// and the WRAPS verification key are set before the new data is published.
    ///
    /// @param updatedTssData the new TSS data, must not be null
    /// @param sendUpdateToAppState if `true`, a successful update is also forwarded
    ///     to the application state facility
    private void updateTssData(final TssData updatedTssData, final boolean sendUpdateToAppState) {
        final TssData localCurrentTss = currentTssData.get();
        // Only update if we see new TSS data
        if (!updatedTssData.equals(localCurrentTss)) {
            LOGGER.log(INFO, "Updating TSS data from application state");
            if (localCurrentTss == null || updatedTssData.validFromBlock() > localCurrentTss.validFromBlock()) {
                final TssRoster roster = updatedTssData.currentRoster();
                if (roster != null) {
                    final List<RosterEntry> contributions = roster.rosterEntries();
                    if (contributions != null && !contributions.isEmpty()) {
                        final int nodeCount = contributions.size();
                        byte[][] publicKeys = new byte[nodeCount][];
                        long[] nodeIds = new long[nodeCount];
                        long[] weights = new long[nodeCount];
                        for (int i = 0; i < nodeCount; i++) {
                            final RosterEntry contribution = contributions.get(i);
                            publicKeys[i] = contribution.schnorrPublicKey().toByteArray();
                            nodeIds[i] = contribution.nodeId();
                            weights[i] = contribution.weight();
                        }
                        TSS.setAddressBook(publicKeys, weights, nodeIds);
                        WRAPSVerificationKey.setCurrentKey(
                                updatedTssData.wrapsVerificationKey().toByteArray());
                        if (currentTssData.compareAndSet(localCurrentTss, updatedTssData)) {
                            if (sendUpdateToAppState) {
                                context.applicationStateFacility().updateTssData(currentTssData.get());
                            }
                            LOGGER.log(INFO, "Successfully updated TSS data");
                        } else {
                            LOGGER.log(INFO, "Failed to CAS TSS data from application state");
                        }
                    } else {
                        LOGGER.log(INFO, "No contributions in TSS data roster found");
                    }
                } else {
                    LOGGER.log(INFO, "No roster in TSS data found");
                }
            }
        }
    }

    /// Builds an immutable `node_id → PublicKey` map from the given address book,
    /// used by the RSA WRB verification path.
    ///
    /// Keys are expected to be hex-encoded DER (X.509 `SubjectPublicKeyInfo`) or DER
    /// certificate bytes, **without** a `0x` prefix. Entries where `rsaPubKey()` is
    /// blank or whose key bytes cannot be decoded as an RSA X.509 public key are
    /// skipped with a WARN log. This matches the fail-soft behaviour required by
    /// the verification path: one bad key must not prevent the other nodes from
    /// being counted.
    ///
    /// This logic mirrors `SigFileUtils.decodePublicKey` in `tools-and-tests/tools`
    /// but is inlined here because that module cannot be imported from
    /// `block-node/block-verification`.
    ///
    /// @param book the address book covering the era of the block being verified
    /// @return an unmodifiable map from node ID to [PublicKey]
    /// @throws NoSuchAlgorithmException if the RSA key factory is unavailable
    private Map<Long, PublicKey> buildKeyMap(final NodeAddressBook book) throws NoSuchAlgorithmException {
        if (book == null || book.nodeAddress().isEmpty()) return Map.of();

        final List<NodeAddress> nodeAddresses = book.nodeAddress();
        final Map<Long, PublicKey> map = new HashMap<>();
        final HexFormat hex = HexFormat.of();
        // Obtain KeyFactory once - provider lookup is not cheap and RSA must always be available.
        final KeyFactory kf = KeyFactory.getInstance("RSA");
        for (final NodeAddress addr : nodeAddresses) {
            final String pubKey = addr.rsaPubKey();
            if (!pubKey.isBlank()) {
                try {
                    final byte[] keyBytes = hex.parseHex(pubKey);
                    final PublicKey key = kf.generatePublic(new X509EncodedKeySpec(keyBytes));
                    map.put(addr.nodeId(), key);
                } catch (final InvalidKeySpecException | IllegalArgumentException e) {
                    LOGGER.log(
                            WARNING, "Malformed RSA_PubKey for node {0} - skipped: {1}", addr.nodeId(), e.getMessage());
                }
            }
        }
        return Collections.unmodifiableMap(map);
    }
}
