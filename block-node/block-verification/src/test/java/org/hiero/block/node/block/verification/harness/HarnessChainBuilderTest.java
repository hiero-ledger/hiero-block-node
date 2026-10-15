// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.harness;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.node.base.BlockHashAlgorithm;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import org.hiero.block.common.hasher.HashAlgorithm;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.app.fixtures.TestConfigurationBuilder;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.async.TestThreadPoolManager;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.block.verification.VerificationDataProvider;
import org.hiero.block.node.block.verification.metrics.MetricsHolder;
import org.hiero.block.node.block.verification.verifier.TSSVerifier;
import org.hiero.block.node.spi.BlockNodeContext;
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
    void threeBlockChainAllVerify() throws ParseException, NoSuchAlgorithmException {
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
    /// the SHA-384 empty tree hash, and the TSS signature verifies over the root. The root itself
    /// is computed by the legacy block root tree, which `legacyBlockRootTreeMatchesProductionHashers`
    /// proves equal to the production hashers.
    @Test
    @DisplayName("Chain built for SHA2_384 carries SHA-384 hashes and verifies")
    void chainForOtherAlgorithmMatchesThatAlgorithm() throws ParseException, NoSuchAlgorithmException {
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
        final TSSVerifier verifier = new TSSVerifier(
                metricsHolder.proofVerificationMetrics(),
                block1.rootHash(),
                extractSignature(block1.block()),
                verificationDataProvider);
        assertThat(verifier.verify()).isNull();
    }

    /// This test aims to assert that the legacy block root tree, which the harness hashes a chain
    /// of another algorithm with, computes exactly what the production hashers compute. For every
    /// block of a SHA2_256 chain, whose signed root comes from the `BlockHasher` and whose footer
    /// carries the root of all previous block hashes from the common `StreamingHasher`, the legacy
    /// tree computing with SHA2_256 returns the identical block root and the identical root of all
    /// previous block hashes; the chain of four blocks folds that tree from zero to three leaves.
    /// The production code computes with SHA2_256 only, so this equality is what keeps a chain
    /// built for SHA2_384 hashed the way a release from before the move to SHA-256 hashes it.
    @Test
    @DisplayName("Legacy block root tree computes the roots the production hashers compute")
    void legacyBlockRootTreeMatchesProductionHashers() throws NoSuchAlgorithmException {
        final HarnessChainBuilder builder =
                new HarnessChainBuilder(TssBlockSigner.create(), verificationDataProvider, metricsHolder);
        final List<HarnessChainBuilder.Signed> chain =
                List.of(builder.genesisWithPublication(), builder.next(1L), builder.next(2L), builder.next(3L));
        final List<byte[]> previousBlockRootHashes = new ArrayList<>();
        for (final HarnessChainBuilder.Signed signed : chain) {
            final long blockNumber = signed.block().number();
            assertThat(Bytes.wrap(LegacyBlockRootTree.treeRoot(HashAlgorithm.SHA2_256, previousBlockRootHashes)))
                    .withFailMessage("Block %d root of all previous block hashes must match", blockNumber)
                    .isEqualTo(signed.block().footer().rootHashOfAllBlockHashesTree());
            assertThat(LegacyBlockRootTree.blockRootHash(HashAlgorithm.SHA2_256, signed.block()))
                    .withFailMessage("Block %d root must match the BlockHasher root", blockNumber)
                    .isEqualTo(signed.rootHash());
            previousBlockRootHashes.add(signed.rootHash().toByteArray());
        }
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
