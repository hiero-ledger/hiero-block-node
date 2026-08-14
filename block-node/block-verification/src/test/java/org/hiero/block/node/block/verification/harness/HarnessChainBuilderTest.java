// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.harness;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.concurrent.LinkedBlockingQueue;
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
