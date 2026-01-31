// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import java.security.PublicKey;
import java.security.Signature;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;
import org.hiero.block.tools.days.model.AddressBookRegistry;
import org.hiero.block.tools.states.postgres.BinaryObjectCsvRow;
import org.hiero.block.tools.states.utils.CryptoUtils;
import picocli.CommandLine.Help.Ansi;

/**
 * A complete saved state, including the signed state and all binary objects referenced by it.
 *
 * @param signedState the signed state
 * @param binaryObjectByHexHashMap map of binary objects by their hex hash. Loaded from Postgres export CSV
 */
public record CompleteSavedState(SignedState signedState, Map<String, BinaryObjectCsvRow> binaryObjectByHexHashMap) {
    /**
     * Validates the complete saved state. Hashing and checking computed hash with stored hash. Then checking hash with
     * signatures using public keys from the address book in state and the OaAddressBook address book. Prints a nicely
     * formatted summary of all checks to the console.
     */
    public void printValidationReport() {
        System.out.println();
        System.out.println(
                Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));
        System.out.println(Ansi.AUTO.string("@|bold,cyan   SAVED STATE VALIDATION REPORT|@"));
        System.out.println(
                Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));

        // === 1. Hash verification ===
        System.out.println(Ansi.AUTO.string("\n@|bold,blue ▶ State Hash Verification|@"));
        System.out.println(Ansi.AUTO.string("@|blue ──────────────────────────────────────────────|@"));

        byte[] readHash = signedState.readHash();
        byte[] computedHash = signedState.generateSignedStateHash(CryptoUtils.getMessageDigest());
        String readHashHex = HexFormat.of().formatHex(readHash);
        String computedHashHex = computedHash != null ? HexFormat.of().formatHex(computedHash) : "null";
        boolean hashMatch = computedHash != null && Arrays.equals(readHash, computedHash);

        System.out.println(Ansi.AUTO.string(String.format("  Stored Hash:   @|cyan %s|@", truncateHex(readHashHex))));
        System.out.println(
                Ansi.AUTO.string(String.format("  Computed Hash: @|cyan %s|@", truncateHex(computedHashHex))));
        if (hashMatch) {
            System.out.println(Ansi.AUTO.string("  @|bold,green ✓ Hashes match|@"));
        } else {
            System.out.println(Ansi.AUTO.string("  @|bold,red ✗ Hashes DO NOT match|@"));
        }

        // === 2. Address book info ===
        AddressBook stateAddressBook = signedState.sigSet().addressBook();
        System.out.println(Ansi.AUTO.string(
                String.format("\n@|bold,blue ▶ Address Book|@ (@|yellow %d|@ members)", stateAddressBook.getSize())));
        System.out.println(Ansi.AUTO.string("@|blue ──────────────────────────────────────────────|@"));
        for (int i = 0; i < stateAddressBook.getSize(); i++) {
            Address addr = stateAddressBook.getAddress(i);
            if (addr != null) {
                System.out.println(Ansi.AUTO.string(String.format(
                        "    Node %d: @|cyan %s|@ (stake: @|yellow %d|@)", i, addr.nickname(), addr.stake())));
            }
        }

        // === 2b. Compare state address book against genesis address book ===
        boolean genesisKeyMatch = true;
        try {
            NodeAddressBook genesisBook = AddressBookRegistry.loadGenesisAddressBook();
            System.out.println(Ansi.AUTO.string(String.format(
                    "\n@|bold,blue ▶ Genesis Address Book Comparison|@ (genesis has @|yellow %d|@ nodes)",
                    genesisBook.nodeAddress().size())));
            System.out.println(Ansi.AUTO.string("@|blue ──────────────────────────────────────────────|@"));

            // Build map of genesis public keys by hex string for matching
            Map<String, Long> genesisPubKeyToAccountId = new HashMap<>();
            for (NodeAddress na : genesisBook.nodeAddress()) {
                String pubKeyHex = na.rsaPubKey().toLowerCase();
                genesisPubKeyToAccountId.put(pubKeyHex, AddressBookRegistry.getNodeAccountId(na));
            }

            int matched = 0;
            int unmatched = 0;
            for (int i = 0; i < stateAddressBook.getSize(); i++) {
                Address addr = stateAddressBook.getAddress(i);
                if (addr == null || addr.sigPublicKey() == null) continue;
                String stateKeyHex = HexFormat.of()
                        .formatHex(addr.sigPublicKey().getEncoded())
                        .toLowerCase();
                Long genesisAccountId = genesisPubKeyToAccountId.remove(stateKeyHex);
                if (genesisAccountId != null) {
                    matched++;
                    System.out.println(Ansi.AUTO.string(String.format(
                            "    @|green ✓|@ State node %d (@|cyan %s|@) matches genesis account 0.0.%d",
                            i, addr.nickname(), genesisAccountId)));
                } else {
                    unmatched++;
                    genesisKeyMatch = false;
                    System.out.println(Ansi.AUTO.string(String.format(
                            "    @|red ✗|@ State node %d (@|cyan %s|@) - @|red no matching key in genesis|@",
                            i, addr.nickname())));
                }
            }
            if (!genesisPubKeyToAccountId.isEmpty()) {
                for (Long accountId : genesisPubKeyToAccountId.values()) {
                    System.out.println(Ansi.AUTO.string(String.format(
                            "    @|yellow ○|@ Genesis account 0.0.%d has no match in state (superset ok)", accountId)));
                }
            }
            System.out.println(Ansi.AUTO.string(String.format(
                    "  Matched: @|yellow %d|@, Unmatched: @|yellow %d|@, Extra in genesis: @|yellow %d|@",
                    matched, unmatched, genesisPubKeyToAccountId.size())));
        } catch (Exception e) {
            genesisKeyMatch = false;
            System.out.println(
                    Ansi.AUTO.string("  @|red ✗ Failed to load genesis address book: " + e.getMessage() + "|@"));
        }

        // === 3. Signature verification ===
        System.out.println(Ansi.AUTO.string("\n@|bold,blue ▶ Signature Verification|@"));
        System.out.println(Ansi.AUTO.string("@|blue ──────────────────────────────────────────────|@"));

        SigSet sigSet = signedState.sigSet();
        System.out.println(Ansi.AUTO.string(String.format("  Total members:    @|yellow %d|@", sigSet.numMembers())));
        System.out.println(Ansi.AUTO.string(String.format("  Signatures:       @|yellow %d|@", sigSet.count())));
        System.out.println(Ansi.AUTO.string(String.format(
                "  Stake collected:  @|yellow %,d|@ / @|yellow %,d|@",
                sigSet.stakeCollected(), stateAddressBook.getTotalStake())));
        System.out.println(Ansi.AUTO.string(String.format(
                "  Supermajority:    @|%s %s|@",
                sigSet.complete() ? "bold,green" : "bold,red", sigSet.complete() ? "✓ Yes" : "✗ No")));

        // verify each signature against state address book
        System.out.println(Ansi.AUTO.string("\n  @|bold Verifying signatures against state address book:|@"));
        int validCount = 0;
        int invalidCount = 0;
        for (int i = 0; i < sigSet.numMembers(); i++) {
            SigInfo sigInfo = sigSet.sigInfo(i);
            if (sigInfo == null) {
                continue;
            }
            Address addr = stateAddressBook.getAddress(i);
            // Use readHash when SigInfo hash is empty (common optimization in later versions)
            byte[] hashToVerify = sigInfo.hash().length > 0 ? sigInfo.hash() : readHash;
            boolean valid = verifySignature(addr != null ? addr.sigPublicKey() : null, hashToVerify, sigInfo.sig());
            if (valid) {
                validCount++;
                System.out.println(Ansi.AUTO.string(String.format(
                        "    @|green ✓|@ Node %d (@|cyan %s|@)", i, addr != null ? addr.nickname() : "unknown")));
            } else {
                invalidCount++;
                System.out.println(Ansi.AUTO.string(String.format(
                        "    @|red ✗|@ Node %d (@|cyan %s|@) - @|red signature invalid|@",
                        i, addr != null ? addr.nickname() : "unknown")));
            }
        }

        // === 4. Signature hash vs state hash ===
        System.out.println(Ansi.AUTO.string("\n@|bold,blue ▶ Signature Hash vs State Hash|@"));
        System.out.println(Ansi.AUTO.string("@|blue ──────────────────────────────────────────────|@"));
        boolean allSigHashesMatchRead = true;
        for (int i = 0; i < sigSet.numMembers(); i++) {
            SigInfo sigInfo = sigSet.sigInfo(i);
            if (sigInfo != null && sigInfo.hash().length > 0 && !Arrays.equals(sigInfo.hash(), readHash)) {
                allSigHashesMatchRead = false;
                System.out.println(Ansi.AUTO.string(
                        String.format("  @|red ✗|@ Node %d signature hash differs from stored state hash", i)));
            }
        }
        if (allSigHashesMatchRead) {
            System.out.println(Ansi.AUTO.string("  @|bold,green ✓ All signature hashes match the stored state hash|@"));
        }

        // === Summary ===
        System.out.println(
                Ansi.AUTO.string("\n@|bold,cyan ════════════════════════════════════════════════════════════|@"));
        System.out.println(Ansi.AUTO.string("@|bold,cyan   VALIDATION SUMMARY|@"));
        System.out.println(
                Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));
        printCheck("State hash matches computed hash", hashMatch);
        printCheck("Supermajority achieved", sigSet.complete());
        printCheck("Signatures valid: " + validCount + "/" + (validCount + invalidCount), invalidCount == 0);
        printCheck("All sig hashes match stored hash", allSigHashesMatchRead);
        printCheck("All state node keys found in genesis address book", genesisKeyMatch);
        System.out.println(Ansi.AUTO.string("@|blue ────────────────────────────────────────────────────────────|@"));
    }

    private static void printCheck(String label, boolean passed) {
        System.out.println(Ansi.AUTO.string(
                String.format("  @|%s %s|@ %s", passed ? "bold,green" : "bold,red", passed ? "✓" : "✗", label)));
    }

    private static String truncateHex(String hex) {
        if (hex.length() <= 32) return hex;
        return hex.substring(0, 16) + "..." + hex.substring(hex.length() - 16);
    }

    private static boolean verifySignature(PublicKey publicKey, byte[] hash, byte[] sig) {
        if (publicKey == null || hash == null || sig == null) return false;
        try {
            Signature signature = Signature.getInstance("SHA384withRSA");
            signature.initVerify(publicKey);
            signature.update(hash);
            return signature.verify(sig);
        } catch (Exception e) {
            return false;
        }
    }
}
