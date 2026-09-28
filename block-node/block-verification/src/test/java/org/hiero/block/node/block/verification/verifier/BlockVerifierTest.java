// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.verifier;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.TssSignedBlockProof;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.block.verification.VerificationDataProvider;
import org.hiero.block.node.block.verification.hasher.HashingResult;
import org.hiero.block.node.block.verification.metrics.ProofVerificationMetrics;
import org.hiero.block.node.block.verification.session.SessionFailureType;
import org.hiero.block.node.block.verification.session.VerificationSessionFailedException;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/// Tests for the [BlockVerifier].
///
/// No verification data is available, so any proof that is actually
/// verified fails with a missing verification data failure; that is enough
/// to tell whether the verifier reached a proof or stopped before it.
@DisplayName("Block Verifier Tests")
class BlockVerifierTest {
    /// The number of the block under test.
    private static final long BLOCK_NUMBER = 10L;
    /// The cancellation flag shared with the verifier under test.
    private AtomicBoolean isCancelled;
    /// The block whose parsed parts make up the hashing result.
    private TestBlock block;
    /// The instance under test.
    private BlockVerifier toTest;

    /// Setup before each test.
    @BeforeEach
    void setUp() {
        isCancelled = new AtomicBoolean(false);
        final BlockNodeContext context =
                new BlockNodeContext(null, null, null, null, null, null, null, null, null, null, null, null, null);
        toTest = new BlockVerifier(
                isCancelled,
                ProofVerificationMetrics.create(TestUtils.createMetrics()),
                System.nanoTime(),
                new VerificationDataProvider(context));
        block = TestBlockBuilder.generateBlockWithNumber(BLOCK_NUMBER);
    }

    /// A hashing result for the test block carrying the given proofs.
    private HashingResult hashingResult(final List<BlockProof> proofs) {
        return new HashingResult(
                BLOCK_NUMBER,
                BlockSource.PUBLISHER,
                block.blockUnparsed(),
                Bytes.wrap(new byte[48]),
                block.header(),
                block.footer(),
                proofs,
                block.hapiVersion());
    }

    /// A recognized TSS block proof that can only fail for missing verification data.
    private static BlockProof tssProof() {
        return BlockProof.newBuilder()
                .signedBlockProof(TssSignedBlockProof.newBuilder()
                        .blockSignature(Bytes.wrap(new byte[96]))
                        .build())
                .build();
    }

    /// Asserts that applying the verifier to the given proofs fails with the given type.
    private void assertFailsWith(final List<BlockProof> proofs, final SessionFailureType expected) {
        final HashingResult input = hashingResult(proofs);
        assertThatThrownBy(() -> toTest.apply(input))
                .isInstanceOf(VerificationSessionFailedException.class)
                .extracting(e -> ((VerificationSessionFailedException) e).getFailureType())
                .isEqualTo(expected);
    }

    /// Tests for the cancellation check between proofs.
    @Nested
    @DisplayName("Cancellation Tests")
    class CancellationTests {
        /// This test aims to assert that a session cancelled before proof
        /// verification starts fails with a cancellation and never verifies a
        /// single proof, which would otherwise fail for missing verification data.
        @Test
        @DisplayName("a cancelled session fails with CANCELLED before any proof is verified")
        void testCancelledSessionFailsBeforeAnyProof() {
            isCancelled.set(true);
            assertFailsWith(List.of(tssProof(), tssProof()), SessionFailureType.CANCELLED);
        }

        /// This test aims to assert that an interrupted thread is treated like
        /// a cancelled session, so a shutdown stops proof verification too.
        @Test
        @DisplayName("an interrupted thread is treated as a cancellation")
        void testInterruptedThreadIsTreatedAsCancelled() {
            Thread.currentThread().interrupt();
            try {
                assertFailsWith(List.of(tssProof()), SessionFailureType.CANCELLED);
            } finally {
                // clear the flag so it does not leak into other tests
                Thread.interrupted();
            }
        }

        /// This test aims to assert that a session which is not cancelled does
        /// verify its proofs: the same input fails for missing verification
        /// data, proving that the cancellation check gates the proof loop and
        /// nothing else.
        @Test
        @DisplayName("a session that is not cancelled verifies its proofs")
        void testUncancelledSessionVerifiesProofs() {
            assertFailsWith(List.of(tssProof()), SessionFailureType.MISSING_VERIFICATION_DATA);
        }
    }

    /// Tests for the selection of proof verifiers.
    @Nested
    @DisplayName("Proof Selection Tests")
    class ProofSelectionTests {
        /// This test aims to assert that a block without any recognized proof
        /// fails with a missing mandatory item, since a block must carry at
        /// least one proof to be verifiable.
        @Test
        @DisplayName("a block without proofs fails with MISSING_MANDATORY_ITEM")
        void testNoProofsFailsAsMissingMandatoryItem() {
            assertFailsWith(List.of(), SessionFailureType.MISSING_MANDATORY_ITEM);
        }
    }
}
