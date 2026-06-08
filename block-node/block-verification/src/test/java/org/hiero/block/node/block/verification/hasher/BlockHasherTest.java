// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.hasher;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.pbj.runtime.ParseException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.block.node.app.fixtures.blocks.ResourceTestBlock;
import org.hiero.block.node.app.fixtures.blocks.ResourceTestBlockBuilder;
import org.hiero.block.node.app.fixtures.blocks.ResourceTestBlockBuilder.StateProof;
import org.hiero.block.node.app.fixtures.blocks.ResourceTestBlockBuilder.WRAPS;
import org.hiero.block.node.app.fixtures.blocks.ResourceTestBlockBuilder.WRB;
import org.hiero.block.node.app.fixtures.blocks.ResourceTestWRBBlock;
import org.hiero.block.node.app.fixtures.plugintest.TestApplicationStateFacility;
import org.hiero.block.node.block.verification.VerificationDataProvider;
import org.hiero.block.node.block.verification.metrics.MetricsHolder;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.metrics.core.MetricRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/// Tests for the [BlockHasher] class.
@Timeout(unit = TimeUnit.SECONDS, value = 5)
@DisplayName("Block Hasher Tests")
class BlockHasherTest {
    private MetricRegistry metricsRegistry;
    private MetricsHolder metrics;
    private BlockNodeContext context;
    private VerificationDataProvider verificationDataProvider;

    /// Setup before each
    @BeforeEach
    void setUp() {
        metricsRegistry = TestUtils.createMetrics();
        metrics = MetricsHolder.create(metricsRegistry);
        context = new BlockNodeContext(
                null,
                metricsRegistry,
                null,
                null,
                null,
                new TestApplicationStateFacility(),
                null,
                null,
                null,
                null,
                null);
        verificationDataProvider = new VerificationDataProvider(context);
    }

    /// This test aims to assert that when a block is fully supplied and we hash it,
    /// the returned [HashingResult] will contain the expected root hash of the block
    /// we want to hash.
    @ParameterizedTest
    @MethodSource("resourceBlocks")
    void testSuccessfulHashingProducesExpectedHash(final ResourceTestBlock block) {
        // Create a new block hasher based on what block we have
        final ConcurrentLinkedDeque<BlockItems> blockItemsDeque = new ConcurrentLinkedDeque<>();
        final BlockHasher toTest = new BlockHasher(
                new AtomicBoolean(false),
                blockItemsDeque,
                metrics.hashingMetrics(),
                block.number(),
                BlockSource.PUBLISHER,
                verificationDataProvider);
        // Supply the block in full to the hasher
        blockItemsDeque.add(block.asBlockItems());
        // Call
        final HashingResult actual = toTest.get();
        // Assert resulting root hash is what we expect
        assertThat(actual).returns(block.blockRootHash(), HashingResult::rootHash);
    }

    /// All available resource blocks.
    private static Stream<Arguments> resourceBlocks() throws IOException, ParseException {
        final List<ResourceTestBlock> wraps = ResourceTestBlockBuilder.loadMultiple(WRAPS.values());
        final List<ResourceTestWRBBlock> wrb = ResourceTestBlockBuilder.loadMultiple(WRB.values());
        final List<ResourceTestBlock> stateProof = ResourceTestBlockBuilder.loadMultiple(StateProof.values());
        return Stream.of(wraps, wrb, stateProof).flatMap(List::stream).map(Arguments::of);
    }
}
