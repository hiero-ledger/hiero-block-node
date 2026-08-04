// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.hasher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.hiero.block.node.base.ParseHelper.standardParse;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.BlockItem.ItemOneOfType;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.FilteredSingleItem;
import com.hedera.hapi.block.stream.RedactedItem;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.BlockHashAlgorithm;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
import org.junit.jupiter.params.provider.ValueSource;

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

        /// This test aims to verify that if an item type this version cannot process is
        /// received, we will fail hashing.
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
                                        SessionFailureType.UNSUPPORTED_ITEM_TYPE,
                                        VerificationSessionFailedException::getFailureType);
                    });
        }

        /// This test aims to assert that when a block with multiple items of specific types is received, we fail.
        /// Some item types are required to only be present once for a block: the items parse
        /// fine, but the stream is not processable.
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
                                        SessionFailureType.UNSUPPORTED_STREAM_FORMAT,
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

    /// Tests for the forward compatibility handling of [BlockHasher]: block item types unknown
    /// to the compiled schema are placed into a hashing category by the block stream forward
    /// compatibility numbering rule (category is the field number modulo 20 for fields numbered
    /// 20 and above). Expected hashes are produced by a local reference implementation of the
    /// HIP-1424 block root tree, independent of the production code.
    @Nested
    @DisplayName("Forward Compatibility Tests")
    class ForwardCompatibilityTests {
        private static final Bytes FUTURE_PAYLOAD = Bytes.wrap("future item payload");
        /// Block number 1 so hashing does not enter the genesis block TSS bootstrap path.
        private static final long BLOCK_NUMBER = 1L;

        /// This test aims to assert that a future item type mapping to a not hashed category
        /// (0 or 19) is read and ignored: the block root hash is identical to the hash of the
        /// same block without the item.
        @ParameterizedTest
        @ValueSource(ints = {20, 39, 40, 59})
        @DisplayName("get() future item in a not hashed category leaves the root hash unchanged")
        void testNotHashedFutureItemLeavesRootUnchanged(final int fieldNumber) throws ParseException {
            final List<BlockItemUnparsed> baseItems = TestBlockBuilder.generateBlockWithNumber(BLOCK_NUMBER)
                    .blockUnparsed()
                    .blockItems();
            final Bytes baseRootHash = hashBlockItems(baseItems).rootHash();
            final List<BlockItemUnparsed> withFutureItem =
                    insertBeforeFooter(baseItems, futureItem(fieldNumber, FUTURE_PAYLOAD));
            final HashingResult actual = hashBlockItems(withFutureItem);
            assertThat(actual.rootHash()).isEqualTo(baseRootHash);
        }

        /// This test aims to assert that a future item type mapping to one of the five defined
        /// category subtrees (categories 3 to 7) is hashed into that subtree exactly like a
        /// known item: the root hash matches the reference implementation and differs from the
        /// hash of the block without the item.
        @ParameterizedTest
        @ValueSource(ints = {23, 24, 25, 26, 27, 43, 47})
        @DisplayName("get() future item in a defined category is hashed into that subtree")
        void testFutureItemHashedIntoExistingSubtree(final int fieldNumber) throws ParseException {
            final List<BlockItemUnparsed> baseItems = TestBlockBuilder.generateBlockWithNumber(BLOCK_NUMBER)
                    .blockUnparsed()
                    .blockItems();
            final Bytes baseRootHash = hashBlockItems(baseItems).rootHash();
            final List<BlockItemUnparsed> withFutureItem =
                    insertBeforeFooter(baseItems, futureItem(fieldNumber, FUTURE_PAYLOAD));
            final HashingResult actual = hashBlockItems(withFutureItem);
            assertThat(actual.rootHash()).isEqualTo(referenceRootHash(withFutureItem));
            assertThat(actual.rootHash()).isNotEqualTo(baseRootHash);
        }

        /// This test aims to assert that a future item type mapping to an extension category
        /// (categories 8 to 15) is hashed into the corresponding extension subtree at leaf
        /// positions 9 to 16 of the block root tree: the root hash matches the reference
        /// implementation and differs from the hash of the block without the item.
        @ParameterizedTest
        @ValueSource(ints = {28, 29, 30, 31, 32, 33, 34, 35, 48, 55})
        @DisplayName("get() future item in an extension category is hashed into the extension subtree")
        void testFutureItemHashedIntoExtensionSubtree(final int fieldNumber) throws ParseException {
            final List<BlockItemUnparsed> baseItems = TestBlockBuilder.generateBlockWithNumber(BLOCK_NUMBER)
                    .blockUnparsed()
                    .blockItems();
            final Bytes baseRootHash = hashBlockItems(baseItems).rootHash();
            final List<BlockItemUnparsed> withFutureItem =
                    insertBeforeFooter(baseItems, futureItem(fieldNumber, FUTURE_PAYLOAD));
            final HashingResult actual = hashBlockItems(withFutureItem);
            assertThat(actual.rootHash()).isEqualTo(referenceRootHash(withFutureItem));
            assertThat(actual.rootHash()).isNotEqualTo(baseRootHash);
        }

        /// This test aims to assert that a block carrying future items across several
        /// categories at once, including repeated items in the same extension subtree and a
        /// not hashed item, produces the root hash computed by the reference implementation.
        @Test
        @DisplayName("get() future items across several categories hash correctly together")
        void testMultipleFutureItemsAcrossSubtrees() throws ParseException {
            final List<BlockItemUnparsed> baseItems = TestBlockBuilder.generateBlockWithNumber(BLOCK_NUMBER)
                    .blockUnparsed()
                    .blockItems();
            List<BlockItemUnparsed> items = baseItems;
            for (final int fieldNumber : new int[] {23, 28, 31, 35, 35, 40}) {
                items = insertBeforeFooter(items, futureItem(fieldNumber, Bytes.wrap("payload " + fieldNumber)));
            }
            final HashingResult actual = hashBlockItems(items);
            assertThat(actual.rootHash()).isEqualTo(referenceRootHash(items));
        }

        /// This test aims to assert that a future item type this version cannot process
        /// refuses the block: categories 1 and 2 (requires specific handling) and categories
        /// 16 to 18 (reserved, no subtree in the block root tree).
        @ParameterizedTest
        @ValueSource(ints = {21, 22, 36, 37, 38, 41, 42})
        @DisplayName("get() future item in a reserved or specific handling category refuses the block")
        void testUnsupportedFutureItemRefusesBlock(final int fieldNumber) throws ParseException {
            assertHashingFails(futureItem(fieldNumber, FUTURE_PAYLOAD), SessionFailureType.UNSUPPORTED_ITEM_TYPE);
        }

        /// This test aims to assert that an unknown field numbered below 20 refuses the block:
        /// such fields are first release fields reserved for item types that require specific
        /// handling this version does not know.
        @ParameterizedTest
        @ValueSource(ints = {13, 18})
        @DisplayName("get() unknown first release field refuses the block")
        void testUnknownFirstReleaseFieldRefusesBlock(final int fieldNumber) throws ParseException {
            assertHashingFails(futureItem(fieldNumber, FUTURE_PAYLOAD), SessionFailureType.UNSUPPORTED_ITEM_TYPE);
        }

        /// This test aims to assert that an item carrying a known item type together with an
        /// unknown field is rejected: a BlockItem is a protobuf oneof, so a valid item carries
        /// exactly one field, and such an item parses fine but is not a processable stream.
        @Test
        @DisplayName("get() known item type with unknown fields is rejected")
        void testKnownItemWithUnknownFieldRejected() throws ParseException {
            final Bytes knownAndUnknown = Bytes.merge(
                    // transaction_result is field 5, a known item type
                    fieldBytes(5, Bytes.wrap("transaction result bytes")), futureItemBytes(23, FUTURE_PAYLOAD));
            final BlockItemUnparsed item = standardParse(BlockItemUnparsed.PROTOBUF, knownAndUnknown);
            assertHashingFails(item, SessionFailureType.UNSUPPORTED_STREAM_FORMAT);
        }

        /// This test aims to assert that an item carrying more than one unknown field is
        /// rejected: a BlockItem is a protobuf oneof, so a valid item carries exactly one
        /// field, and such an item parses fine but is not a processable stream.
        @Test
        @DisplayName("get() multiple unknown fields are rejected")
        void testMultipleUnknownFieldsRejected() throws ParseException {
            final Bytes twoUnknowns =
                    Bytes.merge(futureItemBytes(23, FUTURE_PAYLOAD), futureItemBytes(27, FUTURE_PAYLOAD));
            final BlockItemUnparsed item = standardParse(BlockItemUnparsed.PROTOBUF, twoUnknowns);
            assertHashingFails(item, SessionFailureType.UNSUPPORTED_STREAM_FORMAT);
        }

        /// This test aims to assert that an item carrying no field at all is refused: there is
        /// nothing valid to process, so something unexpected is happening.
        @Test
        @DisplayName("get() an item with no field at all is refused as unknown error")
        void testEmptyItemRefusedAsUnknownError() {
            final BlockItemUnparsed emptyItem = BlockItemUnparsed.newBuilder().build();
            assertHashingFails(emptyItem, SessionFailureType.UNKNOWN_ERROR);
        }

        /// This test aims to assert that a future item survives the parse and serialize round
        /// trip byte for byte. The leaf hash of a future item is computed over the item's
        /// serialized bytes, so the round trip must reproduce the exact bytes the producing
        /// node hashed.
        @ParameterizedTest
        @ValueSource(ints = {20, 23, 28, 39, 500})
        @DisplayName("future item serialization round trips byte identical")
        void testFutureItemSerializationRoundTripsByteIdentical(final int fieldNumber) throws ParseException {
            final Bytes original = futureItemBytes(fieldNumber, FUTURE_PAYLOAD);
            final BlockItemUnparsed parsed = standardParse(BlockItemUnparsed.PROTOBUF, original);
            assertThat(BlockItemUnparsed.PROTOBUF.toBytes(parsed)).isEqualTo(original);
        }

        /// Runs a [BlockHasher] over the given items supplied as one complete block.
        private HashingResult hashBlockItems(final List<BlockItemUnparsed> items) {
            final ConcurrentLinkedDeque<BlockItems> blockItemsDeque = new ConcurrentLinkedDeque<>();
            final BlockHasher toTest = new BlockHasher(
                    new AtomicBoolean(false),
                    blockItemsDeque,
                    metrics.hashingMetrics(),
                    BLOCK_NUMBER,
                    BlockSource.PUBLISHER,
                    verificationDataProvider);
            blockItemsDeque.add(new BlockItems(items, BLOCK_NUMBER, true, true));
            return toTest.get();
        }

        /// Asserts that hashing a block containing the given item fails with the expected
        /// failure type.
        private void assertHashingFails(final BlockItemUnparsed item, final SessionFailureType expectedFailure) {
            final ConcurrentLinkedDeque<BlockItems> blockItemsDeque = new ConcurrentLinkedDeque<>();
            final BlockSource blockSource = BlockSource.PUBLISHER;
            final BlockHasher toTest = new BlockHasher(
                    new AtomicBoolean(false),
                    blockItemsDeque,
                    metrics.hashingMetrics(),
                    BLOCK_NUMBER,
                    blockSource,
                    verificationDataProvider);
            blockItemsDeque.offer(new BlockItems(List.of(item), BLOCK_NUMBER, false, false));
            assertThatThrownBy(toTest::get)
                    .isInstanceOf(VerificationSessionFailedException.class)
                    .asInstanceOf(type(VerificationSessionFailedException.class))
                    .satisfies(e -> {
                        assertThat(e)
                                .returns(BLOCK_NUMBER, VerificationSessionFailedException::getBlockNumber)
                                .returns(blockSource, VerificationSessionFailedException::getBlockSource)
                                .returns(expectedFailure, VerificationSessionFailedException::getFailureType);
                    });
        }
    }

    /// Returns a copy of the given items with the given item inserted just before the footer.
    private static List<BlockItemUnparsed> insertBeforeFooter(
            final List<BlockItemUnparsed> items, final BlockItemUnparsed toInsert) {
        final List<BlockItemUnparsed> result = new ArrayList<>(items);
        for (int i = 0; i < result.size(); i++) {
            if (result.get(i).hasBlockFooter()) {
                result.add(i, toInsert);
                return result;
            }
        }
        throw new IllegalArgumentException("Items contain no footer");
    }

    /// Builds a block item carrying a single field unknown to the compiled schema, the way an
    /// old node sees a future block item type: the crafted wire bytes are parsed with unknown
    /// field collection enabled.
    private static BlockItemUnparsed futureItem(final int fieldNumber, final Bytes payload) throws ParseException {
        return standardParse(BlockItemUnparsed.PROTOBUF, futureItemBytes(fieldNumber, payload));
    }

    /// Builds the wire bytes of a block item carrying a single field with the given field
    /// number, as a newer producing node would serialize a future item type.
    private static Bytes futureItemBytes(final int fieldNumber, final Bytes payload) {
        return fieldBytes(fieldNumber, payload);
    }

    /// Encodes one length delimited protobuf field: tag varint, length varint, payload.
    private static Bytes fieldBytes(final int fieldNumber, final Bytes payload) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final int wireTypeDelimited = 2;
        writeVarInt(out, (fieldNumber << 3) | wireTypeDelimited);
        writeVarInt(out, (int) payload.length());
        out.writeBytes(payload.toByteArray());
        return Bytes.wrap(out.toByteArray());
    }

    /// Writes a protobuf varint to the given stream.
    private static void writeVarInt(final ByteArrayOutputStream out, final int value) {
        int remaining = value;
        while ((remaining & ~0x7F) != 0) {
            out.write((remaining & 0x7F) | 0x80);
            remaining >>>= 7;
        }
        out.write(remaining);
    }

    /// Reference implementation of the block root hash per HIP-1424, independent of the
    /// production hashing code: items are placed into their category (known types by their
    /// fixed assignment, unknown types by field number modulo 20), each category is folded
    /// with the streaming merkle tree algorithm, and the category roots are combined into the
    /// fixed 16 leaf block root tree with absent extension leaves excluded and single child
    /// nodes prefixed with 0x01.
    private static Bytes referenceRootHash(final List<BlockItemUnparsed> items) throws ParseException {
        final List<byte[]> consensusLeaves = new ArrayList<>();
        final List<byte[]> inputLeaves = new ArrayList<>();
        final List<byte[]> outputLeaves = new ArrayList<>();
        final List<byte[]> stateChangesLeaves = new ArrayList<>();
        final List<byte[]> traceLeaves = new ArrayList<>();
        final List<List<byte[]>> extensionLeaves = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            extensionLeaves.add(new ArrayList<>());
        }
        BlockFooter footer = null;
        Timestamp timestamp = null;
        for (final BlockItemUnparsed item : items) {
            final byte[] leaf = refLeaf(BlockItemUnparsed.PROTOBUF.toBytes(item).toByteArray());
            switch (item.item().kind()) {
                case BLOCK_HEADER -> {
                    timestamp = standardParse(BlockHeader.PROTOBUF, item.blockHeader())
                            .blockTimestamp();
                    outputLeaves.add(leaf);
                }
                case ROUND_HEADER, EVENT_HEADER -> consensusLeaves.add(leaf);
                case SIGNED_TRANSACTION -> inputLeaves.add(leaf);
                case TRANSACTION_RESULT, TRANSACTION_OUTPUT, RECORD_FILE -> outputLeaves.add(leaf);
                case STATE_CHANGES -> stateChangesLeaves.add(leaf);
                case TRACE_DATA -> traceLeaves.add(leaf);
                case BLOCK_FOOTER -> footer = standardParse(BlockFooter.PROTOBUF, item.blockFooter());
                case BLOCK_PROOF -> {
                    // not hashed
                }
                case UNSET -> {
                    final int category = item.getUnknownFields().getFirst().field() % 20;
                    switch (category) {
                        case 0, 19 -> {
                            // not hashed
                        }
                        case 3 -> consensusLeaves.add(leaf);
                        case 4 -> inputLeaves.add(leaf);
                        case 5 -> outputLeaves.add(leaf);
                        case 6 -> stateChangesLeaves.add(leaf);
                        case 7 -> traceLeaves.add(leaf);
                        default -> extensionLeaves.get(category - 8).add(leaf);
                    }
                }
                default ->
                    throw new IllegalArgumentException(
                            "Unsupported item type: " + item.item().kind());
            }
        }
        final byte[] leftHalf = refNode(
                refNode(
                        refNode(
                                footer.previousBlockRootHash().toByteArray(),
                                footer.rootHashOfAllBlockHashesTree().toByteArray()),
                        refNode(footer.startOfBlockStateRootHash().toByteArray(), refStreamingRoot(consensusLeaves))),
                refNode(
                        refNode(refStreamingRoot(inputLeaves), refStreamingRoot(outputLeaves)),
                        refNode(refStreamingRoot(stateChangesLeaves), refStreamingRoot(traceLeaves))));
        final byte[][] extensionRoots = new byte[8][];
        for (int i = 0; i < 8; i++) {
            extensionRoots[i] = extensionLeaves.get(i).isEmpty() ? null : refStreamingRoot(extensionLeaves.get(i));
        }
        final byte[] rightHalf = refCombineOptional(
                refCombineOptional(
                        refCombineOptional(extensionRoots[0], extensionRoots[1]),
                        refCombineOptional(extensionRoots[2], extensionRoots[3])),
                refCombineOptional(
                        refCombineOptional(extensionRoots[4], extensionRoots[5]),
                        refCombineOptional(extensionRoots[6], extensionRoots[7])));
        final byte[] mountainTop = rightHalf == null ? refSingle(leftHalf) : refNode(leftHalf, rightHalf);
        final byte[] timestampLeaf =
                refLeaf(Timestamp.PROTOBUF.toBytes(timestamp).toByteArray());
        return Bytes.wrap(refNode(timestampLeaf, mountainTop));
    }

    /// Reference streaming merkle tree root over the given leaf hashes: fold complete sibling
    /// pairs as leaves arrive, then fold the remaining open branches right to left. An empty
    /// tree is the hash of a single zero byte.
    private static byte[] refStreamingRoot(final List<byte[]> leafHashes) {
        if (leafHashes.isEmpty()) {
            return refSha384(new byte[] {0x00});
        }
        final List<byte[]> hashList = new ArrayList<>();
        for (int i = 0; i < leafHashes.size(); i++) {
            hashList.add(leafHashes.get(i));
            for (long n = i; (n & 1L) == 1; n >>= 1) {
                final byte[] right = hashList.removeLast();
                final byte[] left = hashList.removeLast();
                hashList.add(refNode(left, right));
            }
        }
        byte[] root = hashList.getLast();
        for (int i = hashList.size() - 2; i >= 0; i--) {
            root = refNode(hashList.get(i), root);
        }
        return root;
    }

    /// Reference hash of an internal node whose children may be absent.
    private static byte[] refCombineOptional(final byte[] left, final byte[] right) {
        final byte[] node;
        if (left == null && right == null) {
            node = null;
        } else if (left == null) {
            node = refSingle(right);
        } else if (right == null) {
            node = refSingle(left);
        } else {
            node = refNode(left, right);
        }
        return node;
    }

    /// Reference leaf hash with the 0x00 domain separation prefix.
    private static byte[] refLeaf(final byte[] data) {
        return refSha384(new byte[] {0x00}, data);
    }

    /// Reference single child internal node hash with the 0x01 domain separation prefix.
    private static byte[] refSingle(final byte[] child) {
        return refSha384(new byte[] {0x01}, child);
    }

    /// Reference two children internal node hash with the 0x02 domain separation prefix.
    private static byte[] refNode(final byte[] left, final byte[] right) {
        return refSha384(new byte[] {0x02}, left, right);
    }

    /// SHA-384 over the concatenation of the given parts.
    private static byte[] refSha384(final byte[]... parts) {
        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-384");
            for (final byte[] part : parts) {
                digest.update(part);
            }
            return digest.digest();
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
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
