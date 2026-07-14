// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.hasher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.BlockItem.ItemOneOfType;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.FilteredSingleItem;
import com.hedera.hapi.block.stream.RedactedItem;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.BlockHashAlgorithm;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.block.node.app.fixtures.blocks.ResourceTestBlock;
import org.hiero.block.node.app.fixtures.blocks.ResourceTestBlockBuilder;
import org.hiero.block.node.app.fixtures.blocks.ResourceTestBlockBuilder.StateProof;
import org.hiero.block.node.app.fixtures.blocks.ResourceTestBlockBuilder.WRAPS;
import org.hiero.block.node.app.fixtures.blocks.ResourceTestBlockBuilder.WRB;
import org.hiero.block.node.app.fixtures.blocks.ResourceTestWRBBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.app.fixtures.plugintest.TestApplicationStateFacility;
import org.hiero.block.node.block.verification.VerificationDataProvider;
import org.hiero.block.node.block.verification.metrics.MetricsHolder;
import org.hiero.block.node.block.verification.session.SessionFailureType;
import org.hiero.block.node.block.verification.session.VerificationSessionFailedException;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.metrics.core.MetricRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/// Tests for the [BlockHasher] class.
@Timeout(unit = TimeUnit.SECONDS, value = 5)
@DisplayName("Block Hasher Tests")
class BlockHasherTest {
    private static final String ALL_RESOURCE_BLOCKS_SOURCE =
            "org.hiero.block.node.block.verification.hasher.BlockHasherTest#allResourceBlocks";
    private static final String ALL_RESOURCE_WRB_BLOCKS_SOURCE =
            "org.hiero.block.node.block.verification.hasher.BlockHasherTest#allResourceWRBBlocks";
    private static final String ALL_RESOURCE_NON_WRB_BLOCKS_SOURCE =
            "org.hiero.block.node.block.verification.hasher.BlockHasherTest#allResourceNonWRBBlocks";
    private static final String FOOTER_WITH_MISSING_VALUES =
            "org.hiero.block.node.block.verification.hasher.BlockHasherTest#footerWithMissingValues";
    private static final String UNSUPPORTED_ITEM_TYPES =
            "org.hiero.block.node.block.verification.hasher.BlockHasherTest#unsupportedItemTypes";
    private static final String ITEM_TYPES_ALLOWED_ONLY_ONCE_PER_BLOCK =
            "org.hiero.block.node.block.verification.hasher.BlockHasherTest#itemTypesAllowedOnlyOncePerBlock";
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
                null,
                null,
                null);
        verificationDataProvider = new VerificationDataProvider(context);
    }

    /// Positive tests for [BlockHasher] class.
    @Nested
    @DisplayName("Positive Block Hasher Tests")
    class PositiveBlockHasherTests {
        /// This test aims to assert that when a block is fully supplied and we hash it, the returned [HashingResult]
        /// will contain the expected block number of the block we want to hash.
        @ParameterizedTest
        @MethodSource(ALL_RESOURCE_BLOCKS_SOURCE)
        @DisplayName("get() successful hashing produces expected block number")
        void testSuccessfulHashingProducesExpectedBlockNumber(final ResourceTestBlock block) {
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
            // Assert
            assertThat(actual).returns(block.number(), HashingResult::blockNumber);
        }

        /// This test aims to assert that when a block is fully supplied and we hash it, the returned [HashingResult]
        /// will contain the expected root hash of the block we want to hash.
        @ParameterizedTest
        @MethodSource(ALL_RESOURCE_BLOCKS_SOURCE)
        @DisplayName("get() successful hashing produces expected block hash")
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
            // Assert
            assertThat(actual).returns(block.blockRootHash(), HashingResult::rootHash);
        }

        /// This test aims to assert that when a block is fully supplied and we hash it, the returned [HashingResult]
        /// will contain the expected source of the block we want to hash.
        @ParameterizedTest
        @MethodSource(ALL_RESOURCE_BLOCKS_SOURCE)
        @DisplayName("get() successful hashing produces expected block source")
        void testSuccessfulHashingProducesExpectedSource(final ResourceTestBlock block) {
            // Create a new block hasher based on what block we have
            final ConcurrentLinkedDeque<BlockItems> blockItemsDeque = new ConcurrentLinkedDeque<>();
            final Random random = new Random();
            final List<BlockSource> sources = List.of(BlockSource.PUBLISHER, BlockSource.BACKFILL);
            final BlockSource source = sources.get(random.nextInt(sources.size()));
            final BlockHasher toTest = new BlockHasher(
                    new AtomicBoolean(false),
                    blockItemsDeque,
                    metrics.hashingMetrics(),
                    block.number(),
                    source,
                    verificationDataProvider);
            // Supply the block in full to the hasher
            blockItemsDeque.add(block.asBlockItems());
            // Call
            final HashingResult actual = toTest.get();
            // Assert
            assertThat(actual).returns(source, HashingResult::blockSource);
        }

        /// This test aims to assert that when a block is fully supplied and we hash it, the returned [HashingResult]
        /// will contain the expected block unparsed of the block we want to hash.
        @ParameterizedTest
        @MethodSource(ALL_RESOURCE_BLOCKS_SOURCE)
        @DisplayName("get() successful hashing produces expected block unparsed")
        void testSuccessfulHashingProducesExpectedBlockUnparsed(final ResourceTestBlock block) {
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
            // Assert
            assertThat(actual).returns(block.blockUnparsed(), HashingResult::block);
        }

        /// This test aims to assert that when a block is fully supplied and we hash it, the returned [HashingResult]
        /// will contain the expected header of the block we want to hash.
        @ParameterizedTest
        @MethodSource(ALL_RESOURCE_BLOCKS_SOURCE)
        @DisplayName("get() successful hashing produces expected block header")
        void testSuccessfulHashingProducesExpectedHeader(final ResourceTestBlock block) {
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
            // Assert
            assertThat(actual).returns(block.header(), HashingResult::blockHeader);
        }

        /// This test aims to assert that when a block is fully supplied and we hash it, the returned [HashingResult]
        /// will contain the expected footer of the block we want to hash.
        @ParameterizedTest
        @MethodSource(ALL_RESOURCE_BLOCKS_SOURCE)
        @DisplayName("get() successful hashing produces expected block footer")
        void testSuccessfulHashingProducesExpectedFooter(final ResourceTestBlock block) {
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
            // Assert
            assertThat(actual).returns(block.footer(), HashingResult::blockFooter);
        }

        /// This test aims to assert that when a block is fully supplied and we hash it, the returned [HashingResult]
        /// will contain the expected proofs of the block we want to hash.
        @ParameterizedTest
        @MethodSource(ALL_RESOURCE_BLOCKS_SOURCE)
        @DisplayName("get() successful hashing produces expected block proofs")
        void testSuccessfulHashingProducesExpectedProofs(final ResourceTestBlock block) {
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
            // Assert
            final List<BlockProof> expected = block.proofs();
            assertThat(actual.blockProofs())
                    .hasSize(expected.size())
                    .containsExactly(expected.toArray(BlockProof[]::new));
        }

        /// This test aims to assert that when a block is fully supplied and we hash it, the returned [HashingResult]
        /// will contain the expected Hapi Version of the block we want to hash.
        @ParameterizedTest
        @MethodSource(ALL_RESOURCE_BLOCKS_SOURCE)
        @DisplayName("get() successful hashing produces expected block hapi version")
        void testSuccessfulHashingProducesExpectedHapiVersion(final ResourceTestBlock block) {
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
            // Assert
            assertThat(actual).returns(block.hapiVersion(), HashingResult::hapiProtoVersion);
        }

        /// This test aims to assert that when a block is fully supplied and we hash it, the returned [HashingResult]
        /// will contain a value for signed payload of the block we want to hash, if the block is WRB.
        @ParameterizedTest
        @MethodSource(ALL_RESOURCE_WRB_BLOCKS_SOURCE)
        @DisplayName("get() successful hashing produces expected block signed payload")
        void testSuccessfulHashingProducesValueForSignedPayload(final ResourceTestBlock block) {
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
            // Assert
            assertThat(actual.signedWRBPayload()).isNotNull().isNotEmpty();
        }

        /// This test aims to assert that when a block is fully supplied and we hash it, the returned [HashingResult]
        /// will not contain a value for signed payload of the block we want to hash, if the block is WRB.
        @ParameterizedTest
        @MethodSource(ALL_RESOURCE_NON_WRB_BLOCKS_SOURCE)
        @DisplayName("get() successful hashing produces expected block signed payload (non WRB)")
        void testSuccessfulHashingProducesNoValueForSignedPayload(final ResourceTestBlock block) {
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
            // Assert
            assertThat(actual.signedWRBPayload()).isNull();
        }
    }

    /// Negative tests for [BlockHasher] class.
    @Nested
    @DisplayName("Negative Block Hasher Tests")
    class NegativeBlockHasherTests {
        /// This test aims to assert that when a block with missing header is supplied, hashing
        /// will result in a failure.
        @Test
        @DisplayName("get() failed hashing when header missing")
        void testMissingHeader() {
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
            final ConcurrentLinkedDeque<BlockItems> blockItemsDeque = new ConcurrentLinkedDeque<>();
            final BlockSource blockSource = BlockSource.PUBLISHER;
            final BlockHasher toTest = new BlockHasher(
                    new AtomicBoolean(false),
                    blockItemsDeque,
                    metrics.hashingMetrics(),
                    block.number(),
                    blockSource,
                    verificationDataProvider);
            final List<BlockItemUnparsed> headerRemoved = block.asBlockItemUnparsedFiltered(i -> !i.hasBlockHeader());
            final BlockItems blockItems = new BlockItems(headerRemoved, block.number(), true, true);
            blockItemsDeque.offer(blockItems);
            assertThatThrownBy(toTest::get)
                    .isInstanceOf(VerificationSessionFailedException.class)
                    .asInstanceOf(type(VerificationSessionFailedException.class))
                    .satisfies(e -> {
                        assertThat(e)
                                .returns(block.number(), VerificationSessionFailedException::getBlockNumber)
                                .returns(blockSource, VerificationSessionFailedException::getBlockSource)
                                .returns(
                                        SessionFailureType.MISSING_MANDATORY_ITEM,
                                        VerificationSessionFailedException::getFailureType);
                    });
        }

        /// This test aims to assert that when a block with missing footer is supplied, hashing
        /// will result in a failure.
        @Test
        @DisplayName("get() failed hashing when footer missing")
        void testMissingFooter() {
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
            final ConcurrentLinkedDeque<BlockItems> blockItemsDeque = new ConcurrentLinkedDeque<>();
            final BlockSource blockSource = BlockSource.PUBLISHER;
            final BlockHasher toTest = new BlockHasher(
                    new AtomicBoolean(false),
                    blockItemsDeque,
                    metrics.hashingMetrics(),
                    block.number(),
                    blockSource,
                    verificationDataProvider);
            final List<BlockItemUnparsed> headerRemoved = block.asBlockItemUnparsedFiltered(i -> !i.hasBlockFooter());
            final BlockItems blockItems = new BlockItems(headerRemoved, block.number(), true, true);
            blockItemsDeque.offer(blockItems);
            assertThatThrownBy(toTest::get)
                    .isInstanceOf(VerificationSessionFailedException.class)
                    .asInstanceOf(type(VerificationSessionFailedException.class))
                    .satisfies(e -> {
                        assertThat(e)
                                .returns(block.number(), VerificationSessionFailedException::getBlockNumber)
                                .returns(blockSource, VerificationSessionFailedException::getBlockSource)
                                .returns(
                                        SessionFailureType.MISSING_MANDATORY_ITEM,
                                        VerificationSessionFailedException::getFailureType);
                    });
        }

        /// This test aims to assert that when a block with no proofs is supplied, hashing
        /// will result in a failure.
        @Test
        @DisplayName("get() failed hashing when proofs missing")
        void testMissingProofs() {
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
            final ConcurrentLinkedDeque<BlockItems> blockItemsDeque = new ConcurrentLinkedDeque<>();
            final BlockSource blockSource = BlockSource.PUBLISHER;
            final BlockHasher toTest = new BlockHasher(
                    new AtomicBoolean(false),
                    blockItemsDeque,
                    metrics.hashingMetrics(),
                    block.number(),
                    blockSource,
                    verificationDataProvider);
            final List<BlockItemUnparsed> headerRemoved = block.asBlockItemUnparsedFiltered(i -> !i.hasBlockProof());
            final BlockItems blockItems = new BlockItems(headerRemoved, block.number(), true, true);
            blockItemsDeque.offer(blockItems);
            assertThatThrownBy(toTest::get)
                    .isInstanceOf(VerificationSessionFailedException.class)
                    .asInstanceOf(type(VerificationSessionFailedException.class))
                    .satisfies(e -> {
                        assertThat(e)
                                .returns(block.number(), VerificationSessionFailedException::getBlockNumber)
                                .returns(blockSource, VerificationSessionFailedException::getBlockSource)
                                .returns(
                                        SessionFailureType.MISSING_MANDATORY_ITEM,
                                        VerificationSessionFailedException::getFailureType);
                    });
        }

        /// This test aims to assert that when a header with missing hapi version is supplied,
        /// the hashing process will fail.
        @Test
        @DisplayName("get() failed hashing when hapi version missing")
        void testMissingHapiVersion() throws ParseException {
            final long blockNumber = 0;
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(blockNumber)
                    .replace(BlockItemUnparsed::hasBlockHeader, headerWithNoValues(blockNumber));
            final ConcurrentLinkedDeque<BlockItems> blockItemsDeque = new ConcurrentLinkedDeque<>();
            final BlockSource blockSource = BlockSource.PUBLISHER;
            final BlockHasher toTest = new BlockHasher(
                    new AtomicBoolean(false),
                    blockItemsDeque,
                    metrics.hashingMetrics(),
                    block.number(),
                    blockSource,
                    verificationDataProvider);
            blockItemsDeque.offer(block.asBlockItems());
            assertThatThrownBy(toTest::get)
                    .isInstanceOf(VerificationSessionFailedException.class)
                    .asInstanceOf(type(VerificationSessionFailedException.class))
                    .satisfies(e -> {
                        assertThat(e)
                                .returns(block.number(), VerificationSessionFailedException::getBlockNumber)
                                .returns(blockSource, VerificationSessionFailedException::getBlockSource)
                                .returns(
                                        SessionFailureType.MISSING_MANDATORY_FIELD,
                                        VerificationSessionFailedException::getFailureType);
                    });
        }

        /// This test aims to assert that when a block with missing header timestamp is supplied, hashing
        /// will result in a failure.
        @Test
        @DisplayName("get() failed hashing when header timestamp missing")
        void testMissingHeaderTimestamp() throws ParseException {
            final long blockNumber = 0;
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(blockNumber)
                    .replace(BlockItemUnparsed::hasBlockHeader, headerWithNoTimestamp(blockNumber));
            final ConcurrentLinkedDeque<BlockItems> blockItemsDeque = new ConcurrentLinkedDeque<>();
            final BlockSource blockSource = BlockSource.PUBLISHER;
            final BlockHasher toTest = new BlockHasher(
                    new AtomicBoolean(false),
                    blockItemsDeque,
                    metrics.hashingMetrics(),
                    block.number(),
                    blockSource,
                    verificationDataProvider);
            blockItemsDeque.offer(block.asBlockItems());
            assertThatThrownBy(toTest::get)
                    .isInstanceOf(VerificationSessionFailedException.class)
                    .asInstanceOf(type(VerificationSessionFailedException.class))
                    .satisfies(e -> {
                        assertThat(e)
                                .returns(block.number(), VerificationSessionFailedException::getBlockNumber)
                                .returns(blockSource, VerificationSessionFailedException::getBlockSource)
                                .returns(
                                        SessionFailureType.MISSING_MANDATORY_FIELD,
                                        VerificationSessionFailedException::getFailureType);
                    });
        }

        /// This test aims to assert that when a block with missing root hash of all block
        /// hashes tree is supplied, hashing will result in a failure.
        @ParameterizedTest
        @MethodSource(FOOTER_WITH_MISSING_VALUES)
        @DisplayName("get() failed hashing when footer values missing")
        void testMissingFooterValues(final BlockItemUnparsed footerWithMissingValue) {
            final long blockNumber = 0;
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(blockNumber)
                    .replace(BlockItemUnparsed::hasBlockFooter, footerWithMissingValue);
            final ConcurrentLinkedDeque<BlockItems> blockItemsDeque = new ConcurrentLinkedDeque<>();
            final BlockSource blockSource = BlockSource.PUBLISHER;
            final BlockHasher toTest = new BlockHasher(
                    new AtomicBoolean(false),
                    blockItemsDeque,
                    metrics.hashingMetrics(),
                    block.number(),
                    blockSource,
                    verificationDataProvider);
            blockItemsDeque.offer(block.asBlockItems());
            assertThatThrownBy(toTest::get)
                    .isInstanceOf(VerificationSessionFailedException.class)
                    .asInstanceOf(type(VerificationSessionFailedException.class))
                    .satisfies(e -> {
                        assertThat(e)
                                .returns(block.number(), VerificationSessionFailedException::getBlockNumber)
                                .returns(blockSource, VerificationSessionFailedException::getBlockSource)
                                .returns(
                                        SessionFailureType.MISSING_MANDATORY_FIELD,
                                        VerificationSessionFailedException::getFailureType);
                    });
        }

        /// This test aims to verify that if an unsupported item type is received, we will fail hashing.
        @ParameterizedTest
        @MethodSource(UNSUPPORTED_ITEM_TYPES)
        @DisplayName("get() fail when getting an unsupported ")
        void testUnsupportedItemTypes(final BlockItemUnparsed unsupportedItem) {
            final ConcurrentLinkedDeque<BlockItems> blockItemsDeque = new ConcurrentLinkedDeque<>();
            final BlockSource blockSource = BlockSource.PUBLISHER;
            final long blockNumber = 0L;
            final BlockHasher toTest = new BlockHasher(
                    new AtomicBoolean(false),
                    blockItemsDeque,
                    metrics.hashingMetrics(),
                    blockNumber,
                    blockSource,
                    verificationDataProvider);
            blockItemsDeque.offer(new BlockItems(List.of(unsupportedItem), blockNumber, false, false));
            assertThatThrownBy(toTest::get)
                    .isInstanceOf(VerificationSessionFailedException.class)
                    .asInstanceOf(type(VerificationSessionFailedException.class))
                    .satisfies(e -> {
                        assertThat(e)
                                .returns(blockNumber, VerificationSessionFailedException::getBlockNumber)
                                .returns(blockSource, VerificationSessionFailedException::getBlockSource)
                                .returns(
                                        SessionFailureType.UNABLE_TO_PARSE,
                                        VerificationSessionFailedException::getFailureType);
                    });
        }

        /// This test aims to assert that when a block with multiple items of specific types is received, we fail.
        /// Some item types are required to only be present once for a block
        @ParameterizedTest
        @MethodSource(ITEM_TYPES_ALLOWED_ONLY_ONCE_PER_BLOCK)
        @DisplayName("get() failed when block has multiple items when only one of specific type is allowed")
        void testNotAllowedMultipleItems(final BlockItemUnparsed item, final long blockNumber) {
            final ConcurrentLinkedDeque<BlockItems> blockItemsDeque = new ConcurrentLinkedDeque<>();
            final BlockSource blockSource = BlockSource.PUBLISHER;
            final BlockHasher toTest = new BlockHasher(
                    new AtomicBoolean(false),
                    blockItemsDeque,
                    metrics.hashingMetrics(),
                    blockNumber,
                    blockSource,
                    verificationDataProvider);
            blockItemsDeque.offer(new BlockItems(List.of(item, item), blockNumber, item.hasBlockHeader(), false));
            assertThatThrownBy(toTest::get)
                    .isInstanceOf(VerificationSessionFailedException.class)
                    .asInstanceOf(type(VerificationSessionFailedException.class))
                    .satisfies(e -> {
                        assertThat(e)
                                .returns(blockNumber, VerificationSessionFailedException::getBlockNumber)
                                .returns(blockSource, VerificationSessionFailedException::getBlockSource)
                                .returns(
                                        SessionFailureType.UNABLE_TO_PARSE,
                                        VerificationSessionFailedException::getFailureType);
                    });
        }

        /// This test aims to assert that when a block where the header is not the first item is received, hashing
        /// will fail
        @Test
        @DisplayName("get() failed when header is not first item")
        void testHeaderNotFirstItem() throws ParseException {
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
            final ConcurrentLinkedDeque<BlockItems> blockItemsDeque = new ConcurrentLinkedDeque<>();
            final BlockSource blockSource = BlockSource.PUBLISHER;
            final BlockHasher toTest = new BlockHasher(
                    new AtomicBoolean(false),
                    blockItemsDeque,
                    metrics.hashingMetrics(),
                    block.number(),
                    blockSource,
                    verificationDataProvider);
            final List<BlockItemUnparsed> headerRemoved = block.asBlockItemUnparsedFiltered(i -> !i.hasBlockHeader());
            final List<BlockItemUnparsed> headerNotFirstItem = new ArrayList<>(headerRemoved);
            headerNotFirstItem.add(TestBlockBuilder.convertToUnparsedItem(
                    BlockItem.newBuilder().blockHeader(block.header()).build()));
            final BlockItems blockItems = new BlockItems(headerNotFirstItem, block.number(), true, true);
            blockItemsDeque.offer(blockItems);
            assertThatThrownBy(toTest::get)
                    .isInstanceOf(VerificationSessionFailedException.class)
                    .asInstanceOf(type(VerificationSessionFailedException.class))
                    .satisfies(e -> {
                        assertThat(e)
                                .returns(block.number(), VerificationSessionFailedException::getBlockNumber)
                                .returns(blockSource, VerificationSessionFailedException::getBlockSource)
                                .returns(
                                        SessionFailureType.MISSING_MANDATORY_ITEM,
                                        VerificationSessionFailedException::getFailureType);
                    });
        }
    }

    private BlockItemUnparsed headerWithNoValues(final long blockNumber) throws ParseException {
        final BlockHeader headerWithNoValues = new BlockHeader(null, null, blockNumber, null, null);
        return TestBlockBuilder.convertToUnparsedItem(
                new BlockItem(new OneOf<>(ItemOneOfType.BLOCK_HEADER, headerWithNoValues)));
    }

    private BlockItemUnparsed headerWithNoTimestamp(final long blockNumber) throws ParseException {
        final BlockHeader headerWithNoTimestamp = new BlockHeader(
                SemanticVersion.DEFAULT, SemanticVersion.DEFAULT, blockNumber, null, BlockHashAlgorithm.SHA2_384);
        return TestBlockBuilder.convertToUnparsedItem(
                new BlockItem(new OneOf<>(ItemOneOfType.BLOCK_HEADER, headerWithNoTimestamp)));
    }

    /// All available resource blocks.
    private static Stream<Arguments> allResourceBlocks() throws IOException, ParseException {
        final List<ResourceTestBlock> wraps = ResourceTestBlockBuilder.loadMultiple(WRAPS.values());
        final List<ResourceTestWRBBlock> wrb = ResourceTestBlockBuilder.loadMultiple(WRB.values());
        final List<ResourceTestBlock> stateProof = ResourceTestBlockBuilder.loadMultiple(StateProof.values());
        return Stream.of(wraps, wrb, stateProof).flatMap(List::stream).map(Arguments::of);
    }

    /// All available resource WRB blocks.
    private static Stream<Arguments> allResourceWRBBlocks() throws IOException, ParseException {
        final List<ResourceTestWRBBlock> wrb = ResourceTestBlockBuilder.loadMultiple(WRB.values());
        return wrb.stream().map(Arguments::of);
    }

    /// All available non WRB resource blocks.
    private static Stream<Arguments> allResourceNonWRBBlocks() throws IOException, ParseException {
        final List<ResourceTestBlock> wraps = ResourceTestBlockBuilder.loadMultiple(WRAPS.values());
        final List<ResourceTestBlock> stateProof = ResourceTestBlockBuilder.loadMultiple(StateProof.values());
        return Stream.of(wraps, stateProof).flatMap(List::stream).map(Arguments::of);
    }

    private static Stream<Arguments> footerWithMissingValues() throws ParseException {
        final Bytes someBytes = Bytes.wrap("someBytes");
        final Bytes empty = Bytes.EMPTY;
        return Stream.of(
                Arguments.of(TestBlockBuilder.convertToUnparsedItem(new BlockItem(
                        new OneOf<>(ItemOneOfType.BLOCK_FOOTER, new BlockFooter(null, someBytes, someBytes))))),
                Arguments.of(TestBlockBuilder.convertToUnparsedItem(new BlockItem(
                        new OneOf<>(ItemOneOfType.BLOCK_FOOTER, new BlockFooter(empty, someBytes, someBytes))))),
                Arguments.of(TestBlockBuilder.convertToUnparsedItem(new BlockItem(
                        new OneOf<>(ItemOneOfType.BLOCK_FOOTER, new BlockFooter(someBytes, null, someBytes))))),
                Arguments.of(TestBlockBuilder.convertToUnparsedItem(new BlockItem(
                        new OneOf<>(ItemOneOfType.BLOCK_FOOTER, new BlockFooter(someBytes, empty, someBytes))))),
                Arguments.of(TestBlockBuilder.convertToUnparsedItem(new BlockItem(
                        new OneOf<>(ItemOneOfType.BLOCK_FOOTER, new BlockFooter(someBytes, someBytes, null))))),
                Arguments.of(TestBlockBuilder.convertToUnparsedItem(new BlockItem(
                        new OneOf<>(ItemOneOfType.BLOCK_FOOTER, new BlockFooter(someBytes, someBytes, empty))))));
    }

    private static Stream<Arguments> unsupportedItemTypes() throws ParseException {
        return Stream.of(
                Arguments.of(TestBlockBuilder.convertToUnparsedItem(new BlockItem(new OneOf<>(
                        ItemOneOfType.REDACTED_ITEM, RedactedItem.newBuilder().build())))),
                Arguments.of(TestBlockBuilder.convertToUnparsedItem(new BlockItem(new OneOf<>(
                        ItemOneOfType.FILTERED_SINGLE_ITEM,
                        FilteredSingleItem.newBuilder().build())))));
    }

    private static Stream<Arguments> itemTypesAllowedOnlyOncePerBlock() throws IOException, ParseException {
        final long blockNumber = 0L;
        final ResourceTestWRBBlock block0WRB = ResourceTestBlockBuilder.load(WRB.SOLO_4N_BLOCK_0);
        final BlockItemUnparsed recordFile =
                block0WRB.blockUnparsed().blockItems().get(1);
        if (recordFile.hasRecordFile()) {
            final BlockItemUnparsed header = TestBlockBuilder.sampleHeaderUnparsed(blockNumber);
            final BlockItemUnparsed footer = TestBlockBuilder.sampleFooterUnparsed(blockNumber);
            return Stream.of(
                    Arguments.of(header, blockNumber),
                    Arguments.of(footer, blockNumber),
                    Arguments.of(recordFile, blockNumber));
        } else {
            throw new IllegalStateException();
        }
    }
}
