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
    private final AtomicReference<Map<Long, PublicKey>> currentRSAPublicKeys;
    private final AtomicReference<NodeAddressBook> currentNodeAddressBook;

    public VerificationDataProvider(final BlockNodeContext context) {
        this.context = Objects.requireNonNull(context);
        this.currentTssData = new AtomicReference<>(null);
        this.currentRSAPublicKeys = new AtomicReference<>(null);
        this.currentNodeAddressBook = new AtomicReference<>(null);
    }

    public TssData currentTssData() {
        return currentTssData.get();
    }

    public boolean hasTssData() {
        return currentTssData.get() != null;
    }

    public Map<Long, PublicKey> currentRSAPublicKeys() {
        final Map<Long, PublicKey> value = currentRSAPublicKeys.get();
        return value == null ? Map.of() : value;
    }

    /// Safely update the TSS data.
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

    /// Safely update the RSA keys.
    public void safeUpdateNodeAddressBook(final NodeAddressBook nodeAddressBook) {
        try {
            if (nodeAddressBook == null) {
                LOGGER.log(INFO, "No NodeAddressBook in current update");
            } else {
                updateNodeAddressBook(nodeAddressBook);
            }
        } catch (final NoSuchAlgorithmException e) {
            LOGGER.log(WARNING, "RSA KeyFactory not available, cannot update RSA key map", e);
        } catch (final RuntimeException e) {
            LOGGER.log(WARNING, "Failed to update RSA key map in verification", e);
        }
    }

    private void updateNodeAddressBook(final NodeAddressBook nodeAddressBook) throws NoSuchAlgorithmException {
        final NodeAddressBook localCurrentNodeAddressBook = currentNodeAddressBook.get();
        if (localCurrentNodeAddressBook == null
                || nodeAddressBook.hashCode() != localCurrentNodeAddressBook.hashCode()
                || !nodeAddressBook.equals(localCurrentNodeAddressBook)) {
            // Count non-blank address book entries - these are the nodes that should be signable.
            final int declaredCount = (int) nodeAddressBook.nodeAddress().stream()
                    .filter(a -> !a.rsaPubKey().isBlank())
                    .count();
            final Map<Long, PublicKey> updatedKeys = buildKeyMap(nodeAddressBook);
            currentRSAPublicKeys.set(updatedKeys);
            if (!currentNodeAddressBook.compareAndSet(localCurrentNodeAddressBook, nodeAddressBook)) {
                // This CAS is always expected to pass. For now, this update is always done as an
                // onContextUpdate, which are always serial.
                LOGGER.log(WARNING, "Failed to CAS NodeAddressBook from application state");
            }
            final int effectiveCount = updatedKeys.size();
            LOGGER.log(
                    INFO, "RSA key map updated: {0}/{1} nodes loaded from address book", effectiveCount, declaredCount);
            if (effectiveCount < declaredCount) {
                // Malformed DER keys were skipped; threshold is calculated against effectiveCount.
                // Fix the address book so all declared nodes can contribute signatures.
                LOGGER.log(
                        WARNING,
                        "RSA key map: {0}/{1} keys decoded successfully; {2} node(s) had malformed"
                                + " hex-DER bytes and cannot contribute signatures. Verification"
                                + " threshold is calculated against the {0} decodable keys.",
                        effectiveCount,
                        declaredCount,
                        declaredCount - effectiveCount);
            }
        }
    }

    /// Utility for decoding RSA public keys from a `NodeAddressBook` into a
    /// `node_id → PublicKey` map used by the RSA WRB verification path.
    /// Keys are expected to be hex-encoded DER (X.509 `SubjectPublicKeyInfo`) or DER
    /// certificate bytes, **without** a `0x` prefix.
    /// This logic mirrors `SigFileUtils.decodePublicKey` in `tools-and-tests/tools`
    /// but is inlined here because that module cannot be imported from
    /// `block-node/application-state`.
    /// ---
    /// Builds an immutable `node_id → PublicKey` map from the given address book.
    /// Entries where `rsaPubKey()` is blank or whose key bytes cannot be decoded
    /// as an RSA X.509 public key are silently skipped (with a WARN log). This
    /// matches the fail-soft behaviour required by the verification path: one bad
    /// key must not prevent the other nodes from being counted.
    ///
    /// @param book the address book loaded by `RsaRosterBootstrapPlugin`
    /// @return an unmodifiable map from node ID to `PublicKey`
    private Map<Long, PublicKey> buildKeyMap(final NodeAddressBook book) throws NoSuchAlgorithmException {
        final Map<Long, PublicKey> result;
        final List<NodeAddress> nodeAddresses = book.nodeAddress();
        if (nodeAddresses.isEmpty()) {
            result = Map.of();
        } else {
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
                                WARNING,
                                "Malformed RSA_PubKey for node {0} - skipped: {1}",
                                addr.nodeId(),
                                e.getMessage());
                    }
                }
            }
            result = Collections.unmodifiableMap(map);
        }
        return result;
    }
}
