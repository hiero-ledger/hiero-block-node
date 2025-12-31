// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ThreadLocalRandom;
import org.hiero.block.node.backfill.client.BackfillSourceConfig;
import org.hiero.block.node.spi.historicalblocks.LongRange;

/**
 * Default node selection strategy that prioritizes nodes by:
 * 1. Earliest available block (to fill gaps from the start)
 * 2. Configured priority (lower number = higher priority)
 * 3. Health score (fewer failures + lower latency = better)
 * 4. Random tie-breaker for load distribution
 *
 * This strategy requires a {@link NodeHealthProvider} to evaluate node health.
 */
public final class PriorityHealthBasedStrategy implements NodeSelectionStrategy {

    private final NodeHealthProvider healthProvider;

    /**
     * Creates a new strategy with the given health provider.
     *
     * @param healthProvider provider for node health information
     */
    public PriorityHealthBasedStrategy(NodeHealthProvider healthProvider) {
        this.healthProvider = healthProvider;
    }

    @Override
    public Optional<NodeSelection> select(
            long startBlock, long gapEnd, @NonNull Map<BackfillSourceConfig, List<LongRange>> availability) {

        if (startBlock > gapEnd) {
            return Optional.empty();
        }

        OptionalLong earliestAvailableStart = findEarliestAvailableStart(startBlock, gapEnd, availability);
        if (earliestAvailableStart.isEmpty()) {
            return Optional.empty();
        }

        List<NodeSelection> candidates =
                candidatesForEarliest(startBlock, gapEnd, earliestAvailableStart.getAsLong(), availability);
        return chooseBestCandidate(candidates);
    }

    /**
     * Locate the earliest start index across all available ranges that can satisfy the request bounds.
     */
    private OptionalLong findEarliestAvailableStart(
            long startBlock, long gapEnd, @NonNull Map<BackfillSourceConfig, List<LongRange>> availability) {
        long earliestAvailableStart = Long.MAX_VALUE;
        for (Map.Entry<BackfillSourceConfig, List<LongRange>> entry : availability.entrySet()) {
            for (LongRange availableRange : entry.getValue()) {
                long candidateStart = Math.max(startBlock, availableRange.start());
                if (candidateStart > availableRange.end() || candidateStart > gapEnd) {
                    continue;
                }
                earliestAvailableStart = Math.min(earliestAvailableStart, candidateStart);
            }
        }

        if (earliestAvailableStart == Long.MAX_VALUE) {
            return OptionalLong.empty();
        }

        return OptionalLong.of(earliestAvailableStart);
    }

    /**
     * Build the list of nodes that can serve the earliest available start.
     */
    private List<NodeSelection> candidatesForEarliest(
            long startBlock,
            long gapEnd,
            long earliestAvailableStart,
            @NonNull Map<BackfillSourceConfig, List<LongRange>> availability) {
        List<NodeSelection> candidates = new ArrayList<>();
        for (Map.Entry<BackfillSourceConfig, List<LongRange>> entry : availability.entrySet()) {
            for (LongRange availableRange : entry.getValue()) {
                long candidateStart = Math.max(startBlock, availableRange.start());
                if (candidateStart != earliestAvailableStart) {
                    continue;
                }
                if (candidateStart > availableRange.end() || candidateStart > gapEnd) {
                    continue;
                }
                candidates.add(new NodeSelection(entry.getKey(), candidateStart));
            }
        }
        return candidates;
    }

    /**
     * Choose the lowest-priority candidate (tie-breaking by health, then randomly) from the supplied list.
     */
    private Optional<NodeSelection> chooseBestCandidate(@NonNull List<NodeSelection> candidates) {
        if (candidates.isEmpty()) {
            return Optional.empty();
        }

        // Filter by lowest priority number (highest priority)
        int bestPriority = candidates.stream()
                .mapToInt(selection -> selection.nodeConfig().priority())
                .min()
                .orElse(Integer.MAX_VALUE);

        List<NodeSelection> bestPriorityCandidates = candidates.stream()
                .filter(c -> c.nodeConfig().priority() == bestPriority)
                .filter(c -> !healthProvider.isInBackoff(c.nodeConfig()))
                .toList();

        if (bestPriorityCandidates.size() == 1) {
            return Optional.of(bestPriorityCandidates.getFirst());
        }

        if (bestPriorityCandidates.isEmpty()) {
            return Optional.empty();
        }

        // Among same-priority nodes, select by health score
        double bestScore = bestPriorityCandidates.stream()
                .mapToDouble(c -> healthProvider.healthScore(c.nodeConfig()))
                .min()
                .orElse(Double.MAX_VALUE);

        List<NodeSelection> bestHealth = bestPriorityCandidates.stream()
                .filter(c -> Double.compare(healthProvider.healthScore(c.nodeConfig()), bestScore) == 0)
                .toList();

        if (bestHealth.size() == 1) {
            return Optional.of(bestHealth.getFirst());
        }

        // Random tie-breaker for load distribution
        int chosenIndex = ThreadLocalRandom.current().nextInt(bestHealth.size());
        return Optional.of(bestHealth.get(chosenIndex));
    }

    /**
     * Provider for node health information used during selection.
     */
    public interface NodeHealthProvider {
        /**
         * Returns whether the node is currently in backoff state.
         */
        boolean isInBackoff(BackfillSourceConfig node);

        /**
         * Returns a health score for the node (lower is better).
         * Considers failure rate and average latency.
         */
        double healthScore(BackfillSourceConfig node);
    }
}
