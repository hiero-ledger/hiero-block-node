// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.time.Instant;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.tools.days.model.AddressBookRegistry;
import org.hiero.block.tools.days.model.NodeStakeRegistry;
import org.hiero.block.tools.records.SigFileUtils;
import org.hiero.block.tools.records.model.parsed.ValidationException;

/**
 * Validates block signatures by verifying RSA signatures (for SignedRecordFileProof) or
 * checking non-empty TSS signatures (for SignedBlockProof).
 *
 * <p>For SignedRecordFileProof, stake-weighted consensus is used when stake data is
 * available: verified stake must be {@code >= ceil(totalStake / 3)}. When no stake data
 * is available (pre-staking era, before ~July 2022), falls back to equal-weight mode
 * where each node counts as weight 1 and threshold is {@code (nodeCount / 3) + 1}.
 *
 * <p>When {@code collectDetailedStats} is enabled, all signatures are verified (no early
 * exit after threshold) to produce complete per-node participation data as
 * {@link SignatureBlockStats}. Stats are stored in a thread-safe map and retrieved via
 * {@link #popBlockStats(long)}.
 *
 * <p>This is a stateless validation — no cross-block state is maintained.
 */
public final class SignatureValidation implements BlockValidation {

    /** The address book registry providing public keys for signature verification. */
    private final AddressBookRegistry addressBookRegistry;

    /** Optional node stake registry for stake-weighted consensus. May be null. */
    private final NodeStakeRegistry nodeStakeRegistry;

    /** Whether to collect detailed per-block stats (disables early exit). */
    private final boolean collectDetailedStats;

    /** Thread-safe map of collected stats, keyed by block number. Null when stats disabled. */
    private final ConcurrentHashMap<Long, SignatureBlockStats> collectedStats;

    /**
     * Creates a new signature validation with equal-weight consensus (no stake data).
     *
     * @param addressBookRegistry the address book registry for public key lookups
     */
    public SignatureValidation(final AddressBookRegistry addressBookRegistry) {
        this(addressBookRegistry, null, false);
    }

    /**
     * Creates a new signature validation with optional stake-weighted consensus and
     * optional detailed stats collection.
     *
     * <p>When {@code collectDetailedStats} is true, all signatures are verified (no early
     * exit) and per-block {@link SignatureBlockStats} are stored for retrieval via
     * {@link #popBlockStats(long)}.
     *
     * @param addressBookRegistry the address book registry for public key lookups
     * @param nodeStakeRegistry the node stake registry for stake weights (may be null for equal-weight)
     * @param collectDetailedStats true to collect detailed stats (disables early exit)
     */
    public SignatureValidation(
            final AddressBookRegistry addressBookRegistry,
            final NodeStakeRegistry nodeStakeRegistry,
            final boolean collectDetailedStats) {
        this.addressBookRegistry = addressBookRegistry;
        this.nodeStakeRegistry = nodeStakeRegistry;
        this.collectDetailedStats = collectDetailedStats;
        this.collectedStats = collectDetailedStats ? new ConcurrentHashMap<>() : null;
    }

    /**
     * Retrieves and removes the collected stats for the given block number.
     * Returns null if stats collection is disabled or no stats exist for the block.
     *
     * @param blockNumber the block number to retrieve stats for
     * @return the stats, or null
     */
    public SignatureBlockStats popBlockStats(long blockNumber) {
        return collectedStats != null ? collectedStats.remove(blockNumber) : null;
    }

    @Override
    public String name() {
        return "Signatures";
    }

    @Override
    public String description() {
        return "Verifies RSA signatures (SignedRecordFileProof) or non-empty TSS signatures (SignedBlockProof)";
    }

    @Override
    public boolean requiresGenesisStart() {
        return false;
    }

    @Override
    public void validate(final BlockUnparsed block, final long blockNumber) throws ValidationException {
        // Find and selectively parse the block proof
        BlockProof blockProof = null;
        try {
            for (final BlockItemUnparsed item : block.blockItems()) {
                if (item.hasBlockProof()) {
                    blockProof = BlockProof.PROTOBUF.parse(item.blockProofOrThrow());
                    break;
                }
            }
        } catch (ParseException e) {
            throw new ValidationException("Block: " + blockNumber + " - Failed to parse BlockProof: " + e.getMessage());
        }
        if (blockProof == null) {
            throw new ValidationException("Block: " + blockNumber + " - No BlockProof found for signature validation");
        }

        if (blockProof.hasSignedRecordFileProof()) {
            validateSignedRecordFileProof(blockNumber, block, blockProof);
        } else if (blockProof.hasSignedBlockProof()) {
            // TSS verification not yet implemented — verify non-empty
            final Bytes blockSig = blockProof.signedBlockProofOrThrow().blockSignature();
            if (blockSig.length() == 0) {
                throw new ValidationException("Block: " + blockNumber + " - Empty TSS block signature");
            }
        } else {
            throw new ValidationException("Block: " + blockNumber + " - Unknown proof type: "
                    + blockProof.proof().kind());
        }
    }

    /**
     * Validates a SignedRecordFileProof by reconstructing the record file hash and verifying
     * RSA signatures from consensus nodes against the address book.
     *
     * @param blockNumber the block number
     * @param block the full block (unparsed)
     * @param blockProof the block proof containing the SignedRecordFileProof
     * @throws ValidationException if signature validation fails
     */
    private void validateSignedRecordFileProof(
            final long blockNumber, final BlockUnparsed block, final BlockProof blockProof) throws ValidationException {
        final var signedRecordFileProof = blockProof.signedRecordFileProofOrThrow();
        final var signatures = signedRecordFileProof.recordFileSignatures();

        if (signatures.isEmpty()) {
            throw new ValidationException("Block: " + blockNumber + " - No signatures in SignedRecordFileProof");
        }

        // Selectively extract signature data and parse BlockHeader from the block
        Bytes recordFileBytes = null;
        BlockHeader blockHeader = null;
        try {
            for (final BlockItemUnparsed item : block.blockItems()) {
                if (item.hasRecordFile()) {
                    recordFileBytes = item.recordFileOrThrow();
                }
                if (item.hasBlockHeader()) {
                    blockHeader = BlockHeader.PROTOBUF.parse(item.blockHeaderOrThrow());
                }
            }
        } catch (ParseException e) {
            throw new ValidationException(
                    "Block: " + blockNumber + " - Failed to parse BlockHeader: " + e.getMessage());
        }
        if (recordFileBytes == null || blockHeader == null) {
            throw new ValidationException(
                    "Block: " + blockNumber + " - Missing RecordFileItem or BlockHeader for signature verification");
        }

        // Extract signature data via wire-format navigation (no Transaction/TransactionRecord parsing)
        final int version = signedRecordFileProof.version();
        final SignatureDataExtractor.SignatureData sigData;
        final byte[] signedHash;
        try {
            sigData = SignatureDataExtractor.extract(recordFileBytes, version);
            signedHash = SignatureDataExtractor.computeSignedHash(version, blockHeader.hapiProtoVersion(), sigData);
        } catch (ParseException e) {
            throw new ValidationException(
                    "Block: " + blockNumber + " - Failed to extract signature data: " + e.getMessage());
        } catch (IOException e) {
            throw new ValidationException(
                    "Block: " + blockNumber + " - Failed to compute signed hash: " + e.getMessage());
        }
        final Instant blockTime = Instant.ofEpochSecond(sigData.creationTimeSeconds(), sigData.creationTimeNanos());

        // Get the address book for this block's timestamp
        final NodeAddressBook addressBook = addressBookRegistry.getAddressBookForBlock(blockTime);
        final int totalNodes = addressBook.nodeAddress().size();

        // Determine stake-weighted vs equal-weight mode.
        // Fall back to equal-weight if no stake data or if total stake is zero
        // (e.g. early NodeStakeUpdate transactions before staking was enabled).
        final NodeStakeRegistry.StakeMapWithTotal stakeResult =
                nodeStakeRegistry != null ? nodeStakeRegistry.getStakeMapWithTotalForBlock(blockTime) : null;
        final Map<Long, Long> stakeMap = stakeResult != null ? stakeResult.stakeMap() : null;
        final long rawTotalStake = stakeResult != null ? stakeResult.totalStake() : 0;
        final boolean stakeWeighted = stakeMap != null && rawTotalStake > 0;
        final long totalStake;
        final long threshold;
        if (stakeWeighted) {
            totalStake = rawTotalStake;
            // Strong minority: ceil(totalStake / 3)
            threshold = (totalStake / 3) + ((totalStake % 3 == 0) ? 0 : 1);
        } else {
            totalStake = totalNodes;
            threshold = (totalNodes / 3) + 1;
        }

        // Verify each signature, tracking unique nodes to avoid counting duplicates.
        // When collecting stats, verify ALL signatures (no early exit) for complete data.
        // Diagnostics are built lazily — only when validation fails.
        final Set<Long> validatedNodes = new HashSet<>();
        long validatedStake = 0;
        final Map<Long, SignatureBlockStats.NodeResult> nodeResults =
                collectDetailedStats ? new LinkedHashMap<>() : null;

        for (final var sig : signatures) {
            final long accountNum = AddressBookRegistry.accountIdForNode(sig.nodeId());
            try {
                final String pubKeyHex = AddressBookRegistry.publicKeyForNode(addressBook, 0, 0, accountNum);
                if (SigFileUtils.verifyRsaSha384(
                        pubKeyHex, signedHash, sig.signaturesBytes().toByteArray())) {
                    if (validatedNodes.add(accountNum)) {
                        final long weight = stakeWeighted ? stakeMap.getOrDefault(sig.nodeId(), 0L) : 1;
                        validatedStake += weight;
                    }
                    if (nodeResults != null) {
                        nodeResults.put(sig.nodeId(), SignatureBlockStats.NodeResult.VERIFIED);
                    }
                    // Early exit only when not collecting detailed stats
                    if (validatedStake >= threshold && !collectDetailedStats) break;
                } else {
                    if (nodeResults != null) {
                        nodeResults.putIfAbsent(sig.nodeId(), SignatureBlockStats.NodeResult.FAILED);
                    }
                }
            } catch (Exception e) {
                if (nodeResults != null) {
                    nodeResults.putIfAbsent(sig.nodeId(), SignatureBlockStats.NodeResult.ERROR);
                }
            }
        }

        if (validatedStake < threshold) {
            // Build diagnostics lazily — re-verify signatures to produce detailed error message.
            // This path is rare (validation failure) so the extra RSA cost is acceptable.
            final StringBuilder detail = new StringBuilder();
            detail.append("Block: ").append(blockNumber);
            if (stakeWeighted) {
                detail.append(" - Insufficient validated stake: ");
                detail.append(validatedStake).append("/").append(totalStake);
                detail.append(" (").append(validatedNodes.size()).append(" nodes)");
                detail.append(", need ").append(threshold).append("/").append(totalStake);
            } else {
                detail.append(" - Insufficient valid signatures: ");
                detail.append(validatedNodes.size()).append(" unique nodes/").append(totalNodes);
                detail.append(" verified, need ").append(threshold).append("/").append(totalNodes);
            }
            detail.append("\n  blockTime=").append(blockTime);
            detail.append(", signatures=").append(signatures.size());
            detail.append(stakeWeighted ? ", mode=stake-weighted" : ", mode=equal-weight");
            detail.append("\n  Per-signature results:\n");
            for (final var sig : signatures) {
                final long accountNum = AddressBookRegistry.accountIdForNode(sig.nodeId());
                try {
                    final String pubKeyHex = AddressBookRegistry.publicKeyForNode(addressBook, 0, 0, accountNum);
                    final String keySnippet =
                            pubKeyHex.length() > 16 ? pubKeyHex.substring(pubKeyHex.length() - 16) : pubKeyHex;
                    if (SigFileUtils.verifyRsaSha384(
                            pubKeyHex, signedHash, sig.signaturesBytes().toByteArray())) {
                        detail.append("  node ")
                                .append(accountNum)
                                .append(" (id=")
                                .append(sig.nodeId())
                                .append("): VERIFIED key=...")
                                .append(keySnippet)
                                .append("\n");
                    } else {
                        detail.append("  node ")
                                .append(accountNum)
                                .append(" (id=")
                                .append(sig.nodeId())
                                .append("): FAILED key=...")
                                .append(keySnippet)
                                .append("\n");
                    }
                } catch (Exception e) {
                    detail.append("  node ")
                            .append(accountNum)
                            .append(" (id=")
                            .append(sig.nodeId())
                            .append("): ERROR ")
                            .append(e.getClass().getSimpleName())
                            .append("\n");
                }
            }
            throw new ValidationException(detail.toString());
        }

        // Store detailed stats on successful validation
        if (collectDetailedStats && nodeResults != null) {
            // Add NOT_PRESENT for address book nodes without signatures
            for (var na : addressBook.nodeAddress()) {
                nodeResults.putIfAbsent(na.nodeId(), SignatureBlockStats.NodeResult.NOT_PRESENT);
            }
            final SignatureBlockStats stats = new SignatureBlockStats(
                    blockNumber,
                    blockTime,
                    stakeWeighted,
                    totalNodes,
                    totalStake,
                    threshold,
                    Set.copyOf(validatedNodes),
                    validatedStake,
                    signatures.size(),
                    stakeMap != null ? Map.copyOf(stakeMap) : Map.of(),
                    Map.copyOf(nodeResults));
            collectedStats.put(blockNumber, stats);
        }
    }
}
