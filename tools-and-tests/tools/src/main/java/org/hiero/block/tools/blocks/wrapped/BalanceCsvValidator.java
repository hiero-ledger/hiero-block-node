// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.wrapped;

import static org.hiero.block.tools.records.SigFileUtils.verifyRsaSha384;
import static org.hiero.block.tools.utils.Sha384.hashSha384;

import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.streams.AllAccountBalances;
import com.hedera.hapi.streams.SignatureFile;
import com.hedera.hapi.streams.SingleAccountBalances;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hedera.pbj.runtime.io.stream.ReadableStreamingData;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.hiero.block.tools.days.model.AddressBookRegistry;
import org.hiero.block.tools.records.model.parsed.ValidationException;
import org.hiero.block.tools.utils.gcp.BalanceFileBucket;
import picocli.CommandLine.Help.Ansi;

/**
 * Validates computed account balances against signed protobuf balance files from GCP.
 *
 * <p>Balance files are published periodically (typically every 15 minutes) and contain
 * a snapshot of all account balances at that timestamp. This validator downloads these
 * files from the mainnet GCP bucket and compares them against the computed balances
 * from block stream processing.
 */
public class BalanceCsvValidator {

    /** The bucket utility for downloading balance files */
    private final BalanceFileBucket bucket;

    /** The address book registry for signature verification (may be null) */
    private final AddressBookRegistry addressBookRegistry;

    /** Whether to verify signatures */
    private final boolean verifySignatures;

    /** Minimum node account ID */
    private final int minNodeAccountId;

    /** Maximum node account ID */
    private final int maxNodeAccountId;

    /** List of balance checkpoints to validate against */
    private final List<Instant> checkpoints = new ArrayList<>();

    /** Index of the next checkpoint to validate */
    private int nextCheckpointIndex = 0;

    /** Validation results for each checkpoint */
    private final List<CheckpointResult> results = new ArrayList<>();

    /**
     * Create a new BalanceCsvValidator.
     *
     * @param cacheDir the cache directory for downloaded files
     * @param minNodeAccountId minimum node account ID
     * @param maxNodeAccountId maximum node account ID
     * @param userProject GCP project for requester-pays (can be null)
     * @param addressBookRegistry address book registry for signature verification (can be null)
     * @param verifySignatures whether to verify signatures
     */
    public BalanceCsvValidator(
            Path cacheDir,
            int minNodeAccountId,
            int maxNodeAccountId,
            String userProject,
            AddressBookRegistry addressBookRegistry,
            boolean verifySignatures) {
        this.bucket = new BalanceFileBucket(true, cacheDir, minNodeAccountId, maxNodeAccountId, userProject);
        this.addressBookRegistry = addressBookRegistry;
        this.verifySignatures = verifySignatures;
        this.minNodeAccountId = minNodeAccountId;
        this.maxNodeAccountId = maxNodeAccountId;
    }

    /**
     * Load balance checkpoints for a date range.
     *
     * @param startDay start day in format "YYYY-MM-DD"
     * @param endDay end day in format "YYYY-MM-DD"
     */
    public void loadCheckpoints(String startDay, String endDay) {
        // Parse start and end dates
        java.time.LocalDate start = java.time.LocalDate.parse(startDay);
        java.time.LocalDate end = java.time.LocalDate.parse(endDay);

        // Iterate through each day and load checkpoints
        for (java.time.LocalDate date = start; !date.isAfter(end); date = date.plusDays(1)) {
            String dayPrefix = date.toString();
            List<Instant> dayCheckpoints = bucket.listBalanceTimestampsForDay(dayPrefix);
            checkpoints.addAll(dayCheckpoints);
        }

        // Sort checkpoints chronologically
        checkpoints.sort(Instant::compareTo);
        System.out.println(Ansi.AUTO.string("@|yellow Balance checkpoints loaded:|@ " + checkpoints.size()
                + " checkpoints from " + startDay + " to " + endDay));
    }

    /**
     * Check if the given block timestamp has passed any balance checkpoints that need validation.
     *
     * @param blockTimestamp the consensus timestamp of the current block
     * @param computedBalances the current computed balance map
     * @throws ValidationException if balance validation fails
     */
    public void checkBlock(Instant blockTimestamp, Map<Long, Long> computedBalances) throws ValidationException {
        // Check if we've passed any checkpoints
        while (nextCheckpointIndex < checkpoints.size()) {
            Instant checkpoint = checkpoints.get(nextCheckpointIndex);
            if (blockTimestamp.isAfter(checkpoint)) {
                // Validate against this checkpoint
                validateCheckpoint(checkpoint, computedBalances);
                nextCheckpointIndex++;
            } else {
                break;
            }
        }
    }

    /**
     * Validate computed balances against a protobuf checkpoint.
     *
     * @param checkpoint the checkpoint timestamp
     * @param computedBalances the computed balance map
     * @throws ValidationException if validation fails
     */
    private void validateCheckpoint(Instant checkpoint, Map<Long, Long> computedBalances) throws ValidationException {
        System.out.println(Ansi.AUTO.string("\n@|cyan Validating balance checkpoint:|@ " + checkpoint));

        // Download and parse the protobuf file
        byte[] pbBytes = bucket.downloadBalanceFile(checkpoint);
        if (pbBytes == null) {
            System.out.println(
                    Ansi.AUTO.string("@|yellow Warning:|@ Balance file not found for checkpoint " + checkpoint));
            results.add(new CheckpointResult(checkpoint, false, "Balance file not found", 0, 0, 0, 0));
            return;
        }

        // Verify signatures if enabled
        int validSignatures = 0;
        if (verifySignatures && addressBookRegistry != null) {
            validSignatures = verifyBalanceFileSignatures(checkpoint, pbBytes);
            NodeAddressBook addressBook = addressBookRegistry.getAddressBookForBlock(checkpoint);
            int totalNodes = addressBook.nodeAddress().size();
            int requiredSignatures = (totalNodes / 3) + 1;

            if (validSignatures < requiredSignatures) {
                System.out.println(Ansi.AUTO.string("@|red ✗ Insufficient signatures:|@ " + validSignatures + " of "
                        + totalNodes + " (need " + requiredSignatures + ")"));
                results.add(
                        new CheckpointResult(checkpoint, false, "Insufficient signatures", 0, 0, 0, validSignatures));
                throw new ValidationException("Balance file signature verification failed at checkpoint " + checkpoint
                        + ": only " + validSignatures + " valid signatures, need " + requiredSignatures);
            }
            System.out.println(Ansi.AUTO.string(
                    "@|green ✓ Signatures verified:|@ " + validSignatures + " of " + totalNodes + " nodes"));
        }

        // Parse protobuf balances
        Map<Long, Long> fileBalances = parseProtobufBalances(pbBytes);

        // Compare balances
        ComparisonResult comparison = compareBalances(fileBalances, computedBalances);

        // Record result
        results.add(new CheckpointResult(
                checkpoint,
                comparison.mismatches.isEmpty(),
                comparison.mismatches.isEmpty() ? "OK" : "Mismatches found",
                fileBalances.size(),
                comparison.matchCount,
                comparison.mismatches.size(),
                validSignatures));

        // Report results
        if (comparison.mismatches.isEmpty()) {
            System.out.println(Ansi.AUTO.string("@|green ✓ All " + fileBalances.size() + " accounts match|@"));
        } else {
            System.out.println(Ansi.AUTO.string("@|red ✗ Found " + comparison.mismatches.size() + " mismatches out of "
                    + fileBalances.size() + " accounts|@"));
            // Print first 10 mismatches
            int shown = 0;
            for (Map.Entry<Long, BalanceMismatch> entry : comparison.mismatches.entrySet()) {
                if (shown++ >= 10) {
                    System.out.println(
                            Ansi.AUTO.string("  @|yellow ... and " + (comparison.mismatches.size() - 10) + " more|@"));
                    break;
                }
                BalanceMismatch m = entry.getValue();
                System.out.println(Ansi.AUTO.string(String.format(
                        "  Account @|cyan %d|@: expected @|yellow %,d|@ but computed @|red %,d|@ (diff: @|red %+,d|@)",
                        entry.getKey(), m.expected, m.computed, m.computed - m.expected)));
            }
            throw new ValidationException("Balance validation failed at checkpoint " + checkpoint + ": "
                    + comparison.mismatches.size() + " mismatches");
        }
    }

    /**
     * Verify balance file signatures.
     *
     * @param checkpoint the checkpoint timestamp
     * @param pbBytes the balance file bytes
     * @return the number of valid signatures
     */
    private int verifyBalanceFileSignatures(Instant checkpoint, byte[] pbBytes) {
        // Compute the hash of the balance file
        byte[] fileHash = hashSha384(pbBytes);

        // Get address book for this timestamp
        NodeAddressBook addressBook = addressBookRegistry.getAddressBookForBlock(checkpoint);

        int validCount = 0;
        // Check signatures from each node
        for (int nodeAccountId = minNodeAccountId; nodeAccountId <= maxNodeAccountId; nodeAccountId++) {
            byte[] sigBytes = bucket.downloadBalanceSignatureFile(checkpoint, nodeAccountId);
            if (sigBytes == null) {
                continue; // No signature from this node
            }

            try {
                // Parse signature file and verify
                if (verifySignatureFile(sigBytes, fileHash, nodeAccountId, addressBook)) {
                    validCount++;
                }
            } catch (Exception e) {
                // Skip invalid signatures
                System.err.println(
                        "Warning: Could not verify signature from node 0.0." + nodeAccountId + ": " + e.getMessage());
            }
        }
        return validCount;
    }

    /**
     * Verify a single signature file.
     *
     * @param sigBytes the signature file bytes
     * @param fileHash the expected file hash
     * @param nodeAccountId the node account ID
     * @param addressBook the address book
     * @return true if signature is valid
     */
    private boolean verifySignatureFile(
            byte[] sigBytes, byte[] fileHash, int nodeAccountId, NodeAddressBook addressBook) throws Exception {
        try (DataInputStream sin = new DataInputStream(new ByteArrayInputStream(sigBytes))) {
            int version = sin.read();
            if (version != 6) {
                // Only version 6 (protobuf) is supported for balance files
                return false;
            }

            // Parse protobuf signature file
            SignatureFile signatureFile = SignatureFile.PROTOBUF.parse(new ReadableStreamingData(sin));
            if (signatureFile.fileSignature() == null) {
                return false;
            }

            // Get the hash from signature file and compare
            byte[] sigFileHash =
                    signatureFile.fileSignature().hashObjectOrThrow().hash().toByteArray();
            if (!java.util.Arrays.equals(sigFileHash, fileHash)) {
                return false;
            }

            // Get signature bytes
            byte[] signature = signatureFile.fileSignature().signature().toByteArray();

            // Get public key for node
            String rsaPubKey = AddressBookRegistry.publicKeyForNode(addressBook, 0, 0, nodeAccountId);
            if (rsaPubKey == null || rsaPubKey.isEmpty()) {
                return false;
            }

            // Verify signature
            return verifyRsaSha384(rsaPubKey, fileHash, signature);
        }
    }

    /**
     * Parse account balances from protobuf bytes.
     *
     * @param pbBytes the protobuf file bytes
     * @return map of account ID to balance
     */
    private Map<Long, Long> parseProtobufBalances(byte[] pbBytes) {
        Map<Long, Long> balances = new HashMap<>();
        try {
            AllAccountBalances allBalances = AllAccountBalances.PROTOBUF.parse(Bytes.wrap(pbBytes));
            for (SingleAccountBalances account : allBalances.allAccounts()) {
                if (account.accountID() != null) {
                    long accountNum = account.accountID().accountNumOrElse(0L);
                    if (accountNum > 0) {
                        balances.put(accountNum, account.hbarBalance());
                    }
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse protobuf balances", e);
        }
        return balances;
    }

    /**
     * Compare file balances with computed balances.
     * Package-private for testing.
     */
    ComparisonResult compareBalances(Map<Long, Long> fileBalances, Map<Long, Long> computedBalances) {
        Map<Long, BalanceMismatch> mismatches = new TreeMap<>();
        int matchCount = 0;

        for (Map.Entry<Long, Long> entry : fileBalances.entrySet()) {
            long accountId = entry.getKey();
            long expected = entry.getValue();
            Long computed = computedBalances.get(accountId);

            if (computed == null) {
                // Account missing from computed balances
                mismatches.put(accountId, new BalanceMismatch(expected, 0L));
            } else if (!computed.equals(expected)) {
                // Balance mismatch
                mismatches.put(accountId, new BalanceMismatch(expected, computed));
            } else {
                matchCount++;
            }
        }

        return new ComparisonResult(matchCount, mismatches);
    }

    /**
     * Print a summary of all checkpoint validations.
     */
    public void printSummary() {
        if (results.isEmpty()) {
            System.out.println(Ansi.AUTO.string("@|yellow No balance checkpoints were validated|@"));
            return;
        }

        System.out.println();
        System.out.println(
                Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));
        System.out.println(Ansi.AUTO.string("@|bold,cyan   BALANCE VALIDATION SUMMARY|@"));
        System.out.println(
                Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));

        long passed = results.stream().filter(r -> r.passed).count();
        long failed = results.size() - passed;

        System.out.println(Ansi.AUTO.string("@|yellow Checkpoints validated:|@ " + results.size()));
        System.out.println(Ansi.AUTO.string("@|green Passed:|@ " + passed));
        if (failed > 0) {
            System.out.println(Ansi.AUTO.string("@|red Failed:|@ " + failed));
        }

        // List failed checkpoints
        if (failed > 0) {
            System.out.println(Ansi.AUTO.string("\n@|red Failed checkpoints:|@"));
            for (CheckpointResult r : results) {
                if (!r.passed) {
                    System.out.println(Ansi.AUTO.string(
                            "  @|red " + r.timestamp + "|@: " + r.message + " (" + r.mismatchCount + " mismatches)"));
                }
            }
        }
    }

    /**
     * Check if all validations passed.
     */
    public boolean allPassed() {
        return results.stream().allMatch(r -> r.passed);
    }

    /** Result of a single checkpoint validation */
    public record CheckpointResult(
            Instant timestamp,
            boolean passed,
            String message,
            int totalAccounts,
            int matchCount,
            int mismatchCount,
            int validSignatures) {}

    /** A balance mismatch between expected and computed values. Package-private for testing. */
    record BalanceMismatch(long expected, long computed) {}

    /** Result of comparing balances. Package-private for testing. */
    record ComparisonResult(int matchCount, Map<Long, BalanceMismatch> mismatches) {}
}
