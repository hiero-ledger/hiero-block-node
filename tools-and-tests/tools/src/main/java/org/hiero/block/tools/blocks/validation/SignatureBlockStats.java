// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import java.time.Instant;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Per-block signature validation statistics, providing richer data than the
 * simple pass/fail result of {@link SignatureValidation#validate}.
 *
 * <p>Collected by {@link SignatureValidation} when detailed stats are enabled,
 * and delivered to {@link SignatureStatsCollector} for per-day aggregation and CSV output.
 *
 * @param blockNumber the block number
 * @param blockTime the block's consensus timestamp
 * @param stakeWeighted true if stake-weighted consensus was used
 * @param totalNodes total nodes in the address book
 * @param totalStake total stake across all nodes (equal to totalNodes in equal-weight mode)
 * @param threshold minimum stake required for consensus
 * @param validatedNodes set of node IDs that produced valid signatures
 * @param validatedStake accumulated stake from validated nodes
 * @param totalSignatures total signature entries in the block proof
 * @param nodeStakes stake weights per node (nodeId to stake); empty in equal-weight mode
 * @param perNodeResults per-node verification outcome (keyed by nodeId)
 */
public record SignatureBlockStats(
        long blockNumber,
        Instant blockTime,
        boolean stakeWeighted,
        int totalNodes,
        long totalStake,
        long threshold,
        Set<Long> validatedNodes,
        long validatedStake,
        int totalSignatures,
        Map<Long, Long> nodeStakes,
        Map<Long, NodeResult> perNodeResults) {

    /** Result of signature verification for a single node. */
    public enum NodeResult {
        /** Signature was verified successfully. */
        VERIFIED,
        /** Signature verification failed (bad signature). */
        FAILED,
        /** Error during verification (e.g. unknown node, key lookup failure). */
        ERROR,
        /** Node is in the address book but no signature was present. */
        NOT_PRESENT
    }

    /**
     * Builds a {@link SignatureBlockStats} from pre-verified signature data. This is used by
     * commands that verify signatures outside of {@link SignatureValidation} (e.g. the wrap
     * command's Stage 1 pre-verification).
     *
     * @param blockNumber the block number
     * @param blockTime the block's consensus timestamp
     * @param verifiedNodeIds node IDs that produced valid signatures
     * @param totalSignatures total signature entries
     * @param addressBookNodeIds all node IDs from the address book
     * @param stakeMap node stake weights (nodeId to stake), or null for equal-weight
     * @return the constructed stats record
     */
    public static SignatureBlockStats fromPreVerifiedData(
            long blockNumber,
            Instant blockTime,
            List<Long> verifiedNodeIds,
            int totalSignatures,
            List<Long> addressBookNodeIds,
            Map<Long, Long> stakeMap) {

        int totalNodes = addressBookNodeIds.size();
        long rawTotalStake = stakeMap != null
                ? stakeMap.values().stream().mapToLong(Long::longValue).sum()
                : 0;
        boolean stakeWeighted = stakeMap != null && rawTotalStake > 0;
        long totalStake = stakeWeighted ? rawTotalStake : totalNodes;
        long threshold = stakeWeighted ? (totalStake / 3) + ((totalStake % 3 == 0) ? 0 : 1) : (totalNodes / 3) + 1;

        Set<Long> uniqueValidated = new HashSet<>();
        long validatedStake = 0;
        Map<Long, NodeResult> perNodeResults = new LinkedHashMap<>();
        for (long nodeId : verifiedNodeIds) {
            if (uniqueValidated.add(nodeId)) {
                long weight = stakeWeighted ? stakeMap.getOrDefault(nodeId, 0L) : 1;
                validatedStake += weight;
            }
            perNodeResults.put(nodeId, NodeResult.VERIFIED);
        }
        for (long nodeId : addressBookNodeIds) {
            perNodeResults.putIfAbsent(nodeId, NodeResult.NOT_PRESENT);
        }

        return new SignatureBlockStats(
                blockNumber,
                blockTime,
                stakeWeighted,
                totalNodes,
                totalStake,
                threshold,
                Set.copyOf(uniqueValidated),
                validatedStake,
                totalSignatures,
                stakeMap != null ? Map.copyOf(stakeMap) : Map.of(),
                Map.copyOf(perNodeResults));
    }
}
