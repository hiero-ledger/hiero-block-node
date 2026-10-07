// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.harness;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.node.base.BlockHashAlgorithm;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.hiero.block.common.hasher.HashAlgorithm;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.app.fixtures.TestConfigurationBuilder;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.async.TestThreadPoolManager;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.block.verification.VerificationDataProvider;
import org.hiero.block.node.block.verification.hasher.BlockHasher;
import org.hiero.block.node.block.verification.metrics.MetricsHolder;
import org.hiero.block.node.block.verification.verifier.TSSVerifier;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.signing.TssBlockSigner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Smoke test: proves {@link HarnessChainBuilder} produces blocks whose chained footers, computed
 * root hashes, and TSS signatures all line up so the real {@link TSSVerifier} accepts each block
 * on a multi-block chain — without touching a single {@code .blk.gz} fixture.
 */
@Timeout(unit = SECONDS, value = 45)
@DisplayName("HarnessChainBuilder end-to-end")
class HarnessChainBuilderTest {

    private MetricsHolder metricsHolder;
    private VerificationDataProvider verificationDataProvider;

    @BeforeEach
    void setUp() {
        final BlockNodeContext context = TestUtils.testContext(
                new TestConfigurationBuilder().getOrCreateConfig(),
                new TestThreadPoolManager<>(
                        new BlockingExecutor(new LinkedBlockingQueue<>()),
                        new ScheduledBlockingExecutor(new LinkedBlockingQueue<>())));
        metricsHolder = MetricsHolder.create(context.metricRegistry());
        verificationDataProvider = new VerificationDataProvider(context);
    }

    @Test
    @DisplayName("Every block in a 3-block chain verifies via TSSVerifier")
    void threeBlockChainAllVerify() throws ParseException {
        final TssBlockSigner signer = TssBlockSigner.create();
        verificationDataProvider.safeUpdateTssData(signer.verificationMaterial().tssData(), false);
        final HarnessChainBuilder builder = new HarnessChainBuilder(signer, verificationDataProvider, metricsHolder);

        final HarnessChainBuilder.Signed block0 = builder.genesisWithPublication();
        final HarnessChainBuilder.Signed block1 = builder.next(1L);
        final HarnessChainBuilder.Signed block2 = builder.next(2L);

        for (final HarnessChainBuilder.Signed signed : new HarnessChainBuilder.Signed[] {block0, block1, block2}) {
            final Bytes signature = extractSignature(signed.block());
            final TSSVerifier verifier = new TSSVerifier(
                    metricsHolder.proofVerificationMetrics(), signed.rootHash(), signature, verificationDataProvider);
            assertThat(verifier.verify())
                    .withFailMessage("Block %d must verify", signed.block().number())
                    .isNull();
        }
    }

    /// This test aims to assert that a chain built for another algorithm than the test blocks
    /// declare, here SHA2_384 as a release from before the move to SHA-256 verifies, is consistent
    /// under that algorithm: every header declares SHA2_384, every root hash and footer hash has
    /// the SHA-384 digest size, block 1 chains to block 0's root, the state root placeholder is
    /// the SHA-384 empty tree hash, a hasher constructed for SHA2_384 recomputes the same root,
    /// and the TSS signature verifies over it.
    @Test
    @DisplayName("Chain built for SHA2_384 carries SHA-384 hashes a SHA2_384 hasher recomputes")
    void chainForOtherAlgorithmMatchesThatAlgorithm() throws ParseException {
        final TssBlockSigner signer = TssBlockSigner.create();
        verificationDataProvider.safeUpdateTssData(signer.verificationMaterial().tssData(), false);
        final HarnessChainBuilder builder =
                new HarnessChainBuilder(signer, verificationDataProvider, metricsHolder, HashAlgorithm.SHA2_384);
        final HarnessChainBuilder.Signed block0 = builder.genesisWithPublication();
        final HarnessChainBuilder.Signed block1 = builder.next(1L);
        final int hashSize = HashAlgorithm.SHA2_384.hashSize();
        assertThat(block0.block().header().hashAlgorithm()).isEqualTo(BlockHashAlgorithm.SHA2_384);
        assertThat(block1.block().header().hashAlgorithm()).isEqualTo(BlockHashAlgorithm.SHA2_384);
        assertThat(block0.rootHash().length()).isEqualTo(hashSize);
        assertThat(block1.rootHash().length()).isEqualTo(hashSize);
        final BlockFooter footer1 = block1.block().footer();
        assertThat(footer1.previousBlockRootHash()).isEqualTo(block0.rootHash());
        assertThat(footer1.rootHashOfAllBlockHashesTree().length()).isEqualTo(hashSize);
        assertThat(footer1.startOfBlockStateRootHash()).isEqualTo(HashAlgorithm.SHA2_384.emptyTreeHash());
        final ConcurrentLinkedDeque<BlockItems> deque = new ConcurrentLinkedDeque<>();
        final BlockHasher hasher = new BlockHasher(
                HashAlgorithm.SHA2_384,
                new AtomicBoolean(false),
                deque,
                metricsHolder.hashingMetrics(),
                1L,
                BlockSource.PUBLISHER,
                verificationDataProvider);
        deque.add(block1.block().asBlockItems());
        assertThat(hasher.get().rootHash()).isEqualTo(block1.rootHash());
        final TSSVerifier verifier = new TSSVerifier(
                metricsHolder.proofVerificationMetrics(),
                block1.rootHash(),
                extractSignature(block1.block()),
                verificationDataProvider);
        assertThat(verifier.verify()).isNull();
    }

    private static Bytes extractSignature(final TestBlock block) throws ParseException {
        for (final BlockItemUnparsed item : block.blockUnparsed().blockItems()) {
            if (item.item().kind() == BlockItemUnparsed.ItemOneOfType.BLOCK_PROOF) {
                final BlockProof proof = BlockProof.PROTOBUF.parse(item.blockProofOrThrow());
                if (proof.hasSignedBlockProof()) {
                    return proof.signedBlockProof().blockSignature();
                }
            }
        }
        throw new IllegalStateException("No signed block proof in generated block " + block.number());
    }
}
