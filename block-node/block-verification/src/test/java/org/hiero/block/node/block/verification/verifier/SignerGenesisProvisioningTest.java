// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.verifier;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.app.fixtures.TestConfigurationBuilder;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.async.TestThreadPoolManager;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.block.verification.VerificationDataProvider;
import org.hiero.block.node.block.verification.hasher.BlockHasher;
import org.hiero.block.node.block.verification.hasher.HashingResult;
import org.hiero.block.node.block.verification.metrics.MetricsHolder;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.signing.TssBlockSigner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// Proves the production genesis self-provisioning path with a locally generated roster.
///
/// A block 0 carrying [TssBlockSigner#genesisLedgerIdSignedTransaction] is streamed through the real
/// [BlockHasher], which extracts the roster and provisions [VerificationDataProvider] with no
/// out-of-band bootstrap file. A subsequent block signed by the same signer then verifies through the
/// real [TSSVerifier].
@Timeout(unit = SECONDS, value = 30)
@DisplayName("Signer genesis self-provisioning end-to-end")
class SignerGenesisProvisioningTest {

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
    @DisplayName("block 0 publication self-provisions, then a later block verifies")
    void genesisPublicationSelfProvisionsAndLaterBlockVerifies() {
        final TssBlockSigner signer = TssBlockSigner.create();

        // Take a well-formed block 0 and insert the ledger-id publication as the first
        // signed-transaction item (right after the header), so the hasher self-provisions from it
        // before reaching the sample transactions.
        final TestBlock block0 = TestBlockBuilder.generateBlockWithNumber(0L);
        final List<BlockItemUnparsed> block0Items =
                new ArrayList<>(block0.blockUnparsed().blockItems());
        block0Items.add(
                1,
                BlockItemUnparsed.newBuilder()
                        .signedTransaction(signer.genesisLedgerIdSignedTransaction())
                        .build());
        hash(new BlockItems(block0Items, 0L, true, true), 0L);

        assertThat(verificationDataProvider.hasTssData())
                .withFailMessage("Block 0 ledger-id publication should self-provision TssData")
                .isTrue();

        // A later block, signed by the same signer, verifies without any manual provisioning.
        final TestBlock block = TestBlockBuilder.generateBlockWithNumber(1L);
        final Bytes rootHash = hash(block.asBlockItems(), block.number()).rootHash();
        final Bytes signature = signer.signBlockProof(block.number(), rootHash)
                .signedBlockProof()
                .blockSignature();

        final TSSVerifier verifier = new TSSVerifier(
                metricsHolder.proofVerificationMetrics(), rootHash, signature, verificationDataProvider);

        assertThat(verifier.verify())
                .withFailMessage("Block signed by the self-provisioned signer should verify")
                .isNull();
    }

    private HashingResult hash(final BlockItems blockItems, final long blockNumber) {
        final ConcurrentLinkedDeque<BlockItems> deque = new ConcurrentLinkedDeque<>();
        final BlockHasher hasher = new BlockHasher(
                new AtomicBoolean(false),
                deque,
                metricsHolder.hashingMetrics(),
                blockNumber,
                BlockSource.PUBLISHER,
                verificationDataProvider);
        deque.add(blockItems);
        return hasher.get();
    }
}
