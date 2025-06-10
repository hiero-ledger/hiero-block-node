// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.config.types;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.hiero.block.simulator.config.data.ConsumerConfig;

public enum SlowDownType {
    NONE {
        @Override
        public Set<Long> apply(ConsumerConfig consumerConfig) {
            return Set.of(); // No slowdown
        }
    },
    FIXED {
        @Override
        public Set<Long> apply(ConsumerConfig consumerConfig) {
            return parseSlowDownForBlockRange(consumerConfig.slowDownForBlockRange());
        }
    },
    RANDOM {
        @Override
        public Set<Long> apply(ConsumerConfig consumerConfig) {
            List<Long> blockRange = parseBlockRange(consumerConfig.slowDownForBlockRange());
            return randomBlockRangeSet(blockRange.get(0), blockRange.get(1));
        }
    },
    RANDOM_WITH_WAIT {
        @Override
        public Set<Long> apply(ConsumerConfig consumerConfig) {
            List<Long> blockRange = parseBlockRange(consumerConfig.slowDownForBlockRange());
            long randomBlocksToWait =
                    new Random().nextLong(blockRange.get(1) - blockRange.get(0) + 1) + blockRange.get(0);
            Set<Long> set = parseSlowDownForBlockRange(consumerConfig.slowDownForBlockRange());
            set.removeIf(value -> value < randomBlocksToWait);
            return parseSlowDownForBlockRange(consumerConfig.slowDownForBlockRange());
        }
    };

    public abstract Set<Long> apply(ConsumerConfig consumerConfig);

    private static Set<Long> parseSlowDownForBlockRange(String slowDownForBlockRange) {
        final List<Long> list = parseBlockRange(slowDownForBlockRange);
        final long start = list.get(0);
        final long end = list.get(1);

        Set<Long> blockRangeSet = new HashSet<>();
        for (long i = start; i <= end; i++) {
            blockRangeSet.add(i);
        }
        return blockRangeSet;
    }

    private static List<Long> parseBlockRange(final String slowDownForBlockRange) {
        if (slowDownForBlockRange == null || slowDownForBlockRange.isBlank()) {
            return List.of();
        }
        final String[] parts = slowDownForBlockRange.split("-");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid range format. Expected format: start-end (e.g., 1-3)");
        }
        try {
            final long start = Long.parseLong(parts[0].trim());
            final long end = Long.parseLong(parts[1].trim());
            if (start > end) {
                throw new IllegalArgumentException("Range start cannot be greater than range end.");
            }

            return List.of(start, end);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Range values must be valid numbers.", e);
        }
    }

    private static Set<Long> randomBlockRangeSet(final long startBlock, final long endBlock) {
        final Random random = new Random();
        long randomStart = random.nextLong((endBlock - startBlock + 1));
        long randomEnd = random.nextLong(endBlock - startBlock + 1);
        if (randomStart > randomEnd) {
            long temp = randomStart;
            randomStart = randomEnd;
            randomEnd = temp;
        }
        System.out.println("Random block range: " + randomStart + "-" + randomEnd);

        Set<Long> blockRangeSet = new HashSet<>();
        for (long i = randomStart; i <= randomEnd; i++) {
            blockRangeSet.add(i);
        }
        return blockRangeSet;
    }
}
